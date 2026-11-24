package duckdb

import (
	"context"
	"fmt"
	"io"
	"runtime"

	"github.com/bububa/duckdbspike/internal/abi"
)

// FileOpenOptions configures how Conn.OpenFile opens a file through DuckDB's
// virtual file system (VFS).
type FileOpenOptions struct {
	Read      bool
	Write     bool
	Create    bool
	CreateNew bool
	Append    bool
}

// File is an open file handle backed by DuckDB's virtual file system (local,
// memory, or extension-provided filesystems). It implements io.Reader,
// io.Writer, io.Seeker, and io.Closer. Callers must call Close when finished;
// any files left open are automatically closed when the parent Conn closes.
type File struct {
	_      noCopy
	conn   *Conn
	handle uintptr
	closed bool
}

var (
	_ io.Reader          = (*File)(nil)
	_ io.Writer          = (*File)(nil)
	_ io.Seeker          = (*File)(nil)
	_ io.Closer          = (*File)(nil)
	_ io.ReadWriteSeeker = (*File)(nil)
)

// OpenFile opens path through DuckDB's client virtual file system (VFS).
func (c *Conn) OpenFile(ctx context.Context, path string, opts FileOpenOptions) (*File, error) {
	return c.openFileWithTx(ctx, nil, path, opts)
}

// OpenFile opens path through DuckDB's client VFS within tx.
func (tx *Tx) OpenFile(ctx context.Context, path string, opts FileOpenOptions) (*File, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxDone
	}
	return tx.conn.openFileWithTx(ctx, tx, path, opts)
}

func (c *Conn) openFileWithTx(ctx context.Context, tx *Tx, path string, opts FileOpenOptions) (*File, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("%w: empty file path", ErrInvalidInput)
	}
	if !opts.Read && !opts.Write && !opts.Create && !opts.CreateNew && !opts.Append {
		return nil, fmt.Errorf("%w: FileOpenOptions requires at least one flag", ErrInvalidInput)
	}
	cpath, err := abi.CString(path)
	if err != nil {
		return nil, fmt.Errorf("%w: path: %v", ErrInvalidInput, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkTxLocked(tx); err != nil {
		return nil, err
	}
	if c.closed {
		return nil, ErrClosed
	}
	api := c.api

	var clientCtx uintptr
	api.ConnectionGetClientContext(c.handle, &clientCtx)
	if clientCtx == 0 {
		return nil, nativeError("connection_get_client_context", abi.ErrorInvalid, "")
	}
	defer api.DestroyClientContext(&clientCtx)

	fs := api.ClientContextGetFileSystem(clientCtx)
	if fs == 0 {
		return nil, nativeError("client_context_get_file_system", abi.ErrorIO, "failed to obtain DuckDB file system")
	}
	defer api.DestroyFileSystem(&fs)

	nativeOpts := api.CreateFileOpenOptions()
	if nativeOpts == 0 {
		return nil, nativeError("create_file_open_options", abi.ErrorOutOfMemory, "")
	}
	defer api.DestroyFileOpenOptions(&nativeOpts)

	flagPairs := []struct {
		flag int32
		on   bool
	}{
		{abi.FileFlagRead, opts.Read},
		{abi.FileFlagWrite, opts.Write},
		{abi.FileFlagCreate, opts.Create},
		{abi.FileFlagCreateNew, opts.CreateNew},
		{abi.FileFlagAppend, opts.Append},
	}
	for _, fp := range flagPairs {
		if fp.on {
			if api.FileOpenOptionsSetFlag(nativeOpts, fp.flag, true) != abi.StateSuccess {
				return nil, nativeError("file_open_options_set_flag", abi.ErrorInvalidInput, "failed to set file open flag")
			}
		}
	}

	var fh uintptr
	state := api.FileSystemOpen(fs, &cpath[0], nativeOpts, &fh)
	runtime.KeepAlive(cpath)
	if state != abi.StateSuccess || fh == 0 {
		errData := api.FileSystemErrorData(fs)
		if fh != 0 {
			api.DestroyFileHandle(&fh)
		}
		if derr := consumeErrorData(api, "file_system_open", errData); derr != nil {
			return nil, derr
		}
		return nil, nativeError("file_system_open", abi.ErrorIO, fmt.Sprintf("failed to open %q", path))
	}

	f := &File{conn: c, handle: fh}
	if c.files == nil {
		c.files = make(map[*File]struct{})
	}
	c.files[f] = struct{}{}
	return f, nil
}

func (f *File) errorLocked(op string) error {
	if err := consumeErrorData(f.conn.api, op, f.conn.api.FileHandleErrorData(f.handle)); err != nil {
		return err
	}
	return nativeError(op, abi.ErrorIO, "")
}

// Read reads up to len(p) bytes from the VFS file handle.
func (f *File) Read(p []byte) (int, error) {
	if f == nil {
		return 0, ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	n := c.api.FileHandleRead(f.handle, &p[0], int64(len(p)))
	runtime.KeepAlive(p)
	if n < 0 {
		return 0, f.errorLocked("file_handle_read")
	}
	if n == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

// Write writes len(p) bytes to the VFS file handle.
func (f *File) Write(p []byte) (int, error) {
	if f == nil {
		return 0, ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	n := c.api.FileHandleWrite(f.handle, &p[0], int64(len(p)))
	runtime.KeepAlive(p)
	if n < 0 {
		return 0, f.errorLocked("file_handle_write")
	}
	if int(n) < len(p) {
		return int(n), io.ErrShortWrite
	}
	return int(n), nil
}

// Tell returns the current byte offset within the VFS file handle.
func (f *File) Tell() (int64, error) {
	if f == nil {
		return 0, ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return 0, ErrClosed
	}
	pos := c.api.FileHandleTell(f.handle)
	if pos < 0 {
		return 0, f.errorLocked("file_handle_tell")
	}
	return pos, nil
}

// Size returns the current size in bytes of the VFS file.
func (f *File) Size() (int64, error) {
	if f == nil {
		return 0, ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return 0, ErrClosed
	}
	sz := c.api.FileHandleSize(f.handle)
	if sz < 0 {
		return 0, f.errorLocked("file_handle_size")
	}
	return sz, nil
}

// Seek sets the offset for the next Read or Write according to whence
// (io.SeekStart, io.SeekCurrent, or io.SeekEnd) and returns the new offset.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f == nil {
		return 0, ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return 0, ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		pos := c.api.FileHandleTell(f.handle)
		if pos < 0 {
			return 0, f.errorLocked("file_handle_tell")
		}
		base = pos
	case io.SeekEnd:
		sz := c.api.FileHandleSize(f.handle)
		if sz < 0 {
			return 0, f.errorLocked("file_handle_size")
		}
		base = sz
	default:
		return 0, fmt.Errorf("%w: invalid seek whence %d", ErrInvalidInput, whence)
	}
	target := base + offset
	if target < 0 {
		return 0, fmt.Errorf("%w: negative seek position %d", ErrInvalidInput, target)
	}
	if c.api.FileHandleSeek(f.handle, target) != abi.StateSuccess {
		return 0, f.errorLocked("file_handle_seek")
	}
	return target, nil
}

// Sync flushes any buffered writes to the underlying storage.
func (f *File) Sync() error {
	if f == nil {
		return ErrClosed
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed || c.closed {
		return ErrClosed
	}
	if c.api.FileHandleSync(f.handle) != abi.StateSuccess {
		return f.errorLocked("file_handle_sync")
	}
	return nil
}

// Close closes and destroys the underlying DuckDB VFS file handle. Idempotent.
func (f *File) Close() error {
	if f == nil {
		return nil
	}
	c := f.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	return f.releaseLocked()
}

func (f *File) releaseLocked() error {
	if f.closed {
		return nil
	}
	f.closed = true
	var closeErr error
	if f.handle != 0 {
		if f.conn.api.FileHandleClose(f.handle) != abi.StateSuccess {
			closeErr = f.errorLocked("file_handle_close")
		}
		f.conn.api.DestroyFileHandle(&f.handle)
	}
	delete(f.conn.files, f)
	return closeErr
}
