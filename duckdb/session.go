package duckdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// ViewSpec binds one persistent catalog view name to a fixed list of sealed
// Parquet files inside the session's input directory. Globs are never used.
type ViewSpec struct {
	Name  string
	Files []string
}

// ReadOnlySessionSpec configures a locked, read-only query session over sealed
// Parquet files prepared inside a SessionWorkspace.
type ReadOnlySessionSpec struct {
	// Budget optionally reserves this session's native memory, thread, and
	// spill slice from a shared BudgetController.
	Budget *BudgetController
	// Config declares per-session memory, thread, spill, and decode limits.
	// Config.SpillDir is ignored and replaced by the workspace's exclusive
	// spill/ directory.
	Config InstanceConfig
	// Views defines persistent views created during trusted catalog init before
	// closing and reopening READ_ONLY. Every file must resolve inside the
	// workspace's input/ directory.
	Views []ViewSpec
}

// SessionWorkspace owns an exclusive query-root directory tree:
//
//	query-root/
//	  input/    sealed Parquet projections (chmod read-only before reopen)
//	  catalog/  temporary DuckDB catalog (query.duckdb)
//	  spill/    exclusive DuckDB temp_directory
//
// Callers write projections into InputDir(), close them, and call OpenReadOnly
// only after upstream integrity verification succeeds. If upstream fails,
// Close removes the entire directory tree without ever opening DuckDB.
type SessionWorkspace struct {
	_          noCopy
	mu         sync.Mutex
	root       string
	inputDir   string
	catalogDir string
	spillDir   string
	handedOff  bool
	closed     bool
}

// NewSessionWorkspace creates an exclusive 0700 directory tree under baseDir
// (or os.TempDir() when baseDir is empty).
func NewSessionWorkspace(baseDir string) (*SessionWorkspace, error) {
	if baseDir != "" {
		if strings.IndexByte(baseDir, 0) >= 0 {
			return nil, fmt.Errorf("%w: baseDir contains NUL", ErrInvalidInput)
		}
		if err := os.MkdirAll(baseDir, 0o700); err != nil {
			return nil, err
		}
	}
	root, err := os.MkdirTemp(baseDir, "duckdb-session-")
	if err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	ws := &SessionWorkspace{
		root:       root,
		inputDir:   filepath.Join(root, "input"),
		catalogDir: filepath.Join(root, "catalog"),
		spillDir:   filepath.Join(root, "spill"),
	}
	for _, d := range []string{ws.inputDir, ws.catalogDir, ws.spillDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			_ = os.RemoveAll(root)
			return nil, err
		}
	}
	return ws, nil
}

// RootDir returns the absolute path of the exclusive session root.
func (w *SessionWorkspace) RootDir() string { return w.root }

// InputDir returns the directory where sealed input Parquet files must reside.
func (w *SessionWorkspace) InputDir() string { return w.inputDir }

// SpillDir returns the exclusive DuckDB spill directory.
func (w *SessionWorkspace) SpillDir() string { return w.spillDir }

// CatalogPath returns the temporary catalog file path.
func (w *SessionWorkspace) CatalogPath() string { return filepath.Join(w.catalogDir, "query.duckdb") }

// Close deletes the workspace tree unless ownership was transferred to a
// ReadOnlySession. Close is idempotent.
func (w *SessionWorkspace) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.closed || w.handedOff {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	root := w.root
	w.mu.Unlock()
	return removeSessionTree(root)
}

// ReadOnlySession owns a temporary DuckDB instance reopened in native
// READ_ONLY mode with locked configuration, exact allowed_paths, disabled
// external access/extensions, and an exclusive spill directory.
//
// Security boundary: native READ_ONLY protects the catalog from writes and
// allowed_paths + enable_external_access=false + lock_configuration=true block
// external file access, globs, extensions, ATTACH, COPY TO input/, and config
// mutations. Two DuckDB behaviors remain allowed by the engine and cannot be
// disabled by configuration: in-memory TEMP tables and COPY into spill/. This
// session is therefore intended only for trusted, parameterized query
// templates, never untrusted arbitrary SQL.
type ReadOnlySession struct {
	_    noCopy
	db   *Database
	root string
	once sync.Once
	err  error
}

// OpenReadOnly seals input files, initializes a persistent temporary catalog
// with fixed absolute Parquet views, checkpoints and fully closes the write
// instance, reopens in READ_ONLY mode with re-applied resource/safety options,
// and locks configuration before returning.
//
// On success, ownership of the workspace directory transfers to the returned
// ReadOnlySession (which deletes it on Close). On failure, the workspace
// remains owned by the caller to Close.
func (w *SessionWorkspace) OpenReadOnly(ctx context.Context, lib *Library, spec ReadOnlySessionSpec) (*ReadOnlySession, error) {
	if w == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lib == nil || lib.api == nil {
		return nil, ErrClosed
	}
	w.mu.Lock()
	if w.closed || w.handedOff {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	w.mu.Unlock()

	views, allowedFiles, err := w.validateViews(spec.Views)
	if err != nil {
		return nil, err
	}
	cfg := spec.Config
	cfg.SpillDir = w.spillDir
	normCfg, _, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	// Phase 1: trusted catalog initialization (read-write).
	catalogPath := w.CatalogPath()
	initOpts := []Option{
		{Name: "threads", Value: "1"},
		{Name: "memory_limit", Value: formatBytesOption(normCfg.NativeMemoryBytes)},
		{Name: "temp_directory", Value: w.spillDir},
		{Name: "autoinstall_known_extensions", Value: "false"},
		{Name: "autoload_known_extensions", Value: "false"},
		{Name: "allow_community_extensions", Value: "false"},
	}
	initDB, err := lib.Open(ctx, catalogPath, initOpts...)
	if err != nil {
		return nil, fmt.Errorf("init catalog open: %w", err)
	}
	if err := initializeCatalogViews(ctx, initDB, views); err != nil {
		return nil, errors.Join(err, initDB.Close())
	}
	if err := initDB.Close(); err != nil {
		return nil, fmt.Errorf("init catalog close: %w", err)
	}
	if _, statErr := os.Stat(catalogPath + ".wal"); !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("catalog WAL still present after checkpoint and close: %v", statErr)
	}

	// Seal input files and catalog at the OS permission level before READ_ONLY reopen.
	if err := sealReadOnlyPaths(w.inputDir, w.catalogDir, allowedFiles, catalogPath); err != nil {
		return nil, err
	}

	// Phase 2: READ_ONLY reopen with explicitly re-applied resource and safety options.
	roCfg := normCfg
	roCfg.ExtraOptions = append(append([]Option(nil), normCfg.ExtraOptions...),
		Option{Name: "access_mode", Value: "READ_ONLY"},
		Option{Name: "autoinstall_known_extensions", Value: "false"},
		Option{Name: "autoload_known_extensions", Value: "false"},
		Option{Name: "allow_community_extensions", Value: "false"},
	)

	var roDB *Database
	if spec.Budget != nil {
		roDB, err = spec.Budget.Open(ctx, lib, catalogPath, roCfg)
	} else {
		_, opts, valErr := roCfg.validate()
		if valErr != nil {
			return nil, valErr
		}
		roDB, err = lib.Open(ctx, catalogPath, opts...)
		if err == nil {
			roDB.defaultDecode = roCfg.DecodeLimits
			roDB.defaultResult = roCfg.ResultLimits
		}
	}
	if err != nil {
		return nil, fmt.Errorf("read-only reopen: %w", err)
	}

	if err := lockReadOnlyInstance(ctx, roDB, allowedFiles); err != nil {
		return nil, errors.Join(err, roDB.Close())
	}

	w.mu.Lock()
	if w.closed || w.handedOff {
		w.mu.Unlock()
		_ = roDB.Close()
		return nil, ErrClosed
	}
	w.handedOff = true
	w.mu.Unlock()

	return &ReadOnlySession{db: roDB, root: w.root}, nil
}

// Connect opens a read-only connection that sees all persistent catalog views
// and inherits the locked configuration and resource limits.
func (s *ReadOnlySession) Connect(ctx context.Context) (*Conn, error) {
	if s == nil || s.db == nil {
		return nil, ErrClosed
	}
	return s.db.Connect(ctx)
}

// Close interrupts and joins all active queries, closes the read-only
// Database, releases any global budget reservation, and deletes the exclusive
// query-root directory tree. Close is idempotent.
func (s *ReadOnlySession) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		var dbErr error
		if s.db != nil {
			dbErr = s.db.Close()
		}
		rmErr := removeSessionTree(s.root)
		s.err = errors.Join(dbErr, rmErr)
	})
	return s.err
}

func (w *SessionWorkspace) validateViews(specs []ViewSpec) ([]ViewSpec, []string, error) {
	if len(specs) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one view is required", ErrInvalidInput)
	}
	seenViews := make(map[string]struct{}, len(specs))
	seenFiles := make(map[string]struct{})
	outViews := make([]ViewSpec, 0, len(specs))
	allowed := make([]string, 0)
	inputPrefix := w.inputDir + string(filepath.Separator)

	for _, vs := range specs {
		if !isSafeSQLIdentifier(vs.Name) {
			return nil, nil, fmt.Errorf("%w: invalid view name %q", ErrInvalidInput, vs.Name)
		}
		lower := strings.ToLower(vs.Name)
		if _, dup := seenViews[lower]; dup {
			return nil, nil, fmt.Errorf("%w: duplicate view name %q", ErrInvalidInput, vs.Name)
		}
		seenViews[lower] = struct{}{}
		if len(vs.Files) == 0 {
			return nil, nil, fmt.Errorf("%w: view %q requires at least one file", ErrInvalidInput, vs.Name)
		}
		resolvedFiles := make([]string, 0, len(vs.Files))
		for _, f := range vs.Files {
			if strings.IndexByte(f, 0) >= 0 || !filepath.IsAbs(f) {
				return nil, nil, fmt.Errorf("%w: view %q file %q must be absolute without NUL", ErrInvalidInput, vs.Name, f)
			}
			resolved, err := filepath.EvalSymlinks(f)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: resolve %q: %v", ErrInvalidInput, f, err)
			}
			if !strings.HasPrefix(resolved, inputPrefix) {
				return nil, nil, fmt.Errorf("%w: file %q escapes session input directory", ErrInvalidInput, f)
			}
			info, err := os.Stat(resolved)
			if err != nil || !info.Mode().IsRegular() {
				return nil, nil, fmt.Errorf("%w: %q is not a regular file", ErrInvalidInput, resolved)
			}
			resolvedFiles = append(resolvedFiles, resolved)
			if _, ok := seenFiles[resolved]; !ok {
				seenFiles[resolved] = struct{}{}
				allowed = append(allowed, resolved)
			}
		}
		outViews = append(outViews, ViewSpec{Name: vs.Name, Files: resolvedFiles})
	}
	slices.Sort(allowed)
	return outViews, allowed, nil
}

func isSafeSQLIdentifier(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		isDigit := c >= '0' && c <= '9'
		if i == 0 && !isLetter {
			return false
		}
		if !isLetter && !isDigit {
			return false
		}
	}
	return true
}

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func initializeCatalogViews(ctx context.Context, db *Database, views []ViewSpec) error {
	conn, err := db.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, v := range views {
		quoted := make([]string, len(v.Files))
		for i, f := range v.Files {
			quoted[i] = sqlStringLiteral(f)
		}
		stmt := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])", v.Name, strings.Join(quoted, ", "))
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create view %s: %w", v.Name, err)
		}
	}
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("checkpoint catalog: %w", err)
	}
	return conn.Close()
}

func lockReadOnlyInstance(ctx context.Context, db *Database, allowedFiles []string) error {
	conn, err := db.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	quoted := make([]string, len(allowedFiles))
	for i, f := range allowedFiles {
		quoted[i] = sqlStringLiteral(f)
	}
	stmts := []string{
		"SET allowed_paths = [" + strings.Join(quoted, ", ") + "]",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("lock config %q: %w", s, err)
		}
	}
	return conn.Close()
}

func sealReadOnlyPaths(inputDir, catalogDir string, files []string, catalogPath string) error {
	for _, f := range files {
		if err := os.Chmod(f, 0o400); err != nil {
			return fmt.Errorf("seal input file: %w", err)
		}
	}
	if err := os.Chmod(inputDir, 0o500); err != nil {
		return fmt.Errorf("seal input dir: %w", err)
	}
	if err := os.Chmod(catalogPath, 0o400); err != nil {
		return fmt.Errorf("seal catalog file: %w", err)
	}
	if err := os.Chmod(catalogDir, 0o500); err != nil {
		return fmt.Errorf("seal catalog dir: %w", err)
	}
	return nil
}

func removeSessionTree(root string) error {
	if root == "" {
		return nil
	}
	// Restore owner write/execute permissions so sealed 0400/0500 paths can be
	// removed cleanly.
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			_ = os.Chmod(p, 0o700)
		} else {
			_ = os.Chmod(p, 0o600)
		}
		return nil
	})
	return os.RemoveAll(root)
}
