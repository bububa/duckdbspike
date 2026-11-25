package duckdb

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

func isUNCPath(p string) bool {
	return strings.HasPrefix(p, `\\`) || strings.HasPrefix(p, "//")
}

// parsePathList splits an OS library/executable search list (such as PATH,
// LD_LIBRARY_PATH, or DYLD_LIBRARY_PATH) and returns clean absolute directories
// in declaration order, filtering out empty, NUL-containing, relative, and
// (when rejectUNC is true) UNC network paths.
func parsePathList(raw string, sep rune, stripQuotes, rejectUNC bool, isAbs func(string) bool) []string {
	if raw == "" {
		return nil
	}
	if isAbs == nil {
		isAbs = filepath.IsAbs
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == sep })
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(part)
		if stripQuotes && len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
			p = strings.TrimSpace(p[1 : len(p)-1])
		}
		if p == "" || strings.IndexByte(p, 0) >= 0 {
			continue
		}
		if rejectUNC && isUNCPath(p) {
			continue
		}
		if !isAbs(p) {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// parseLDSoConf reads Linux ld.so.conf files (and bounded `include` directives)
// purely in Go without invoking ldconfig. Relative paths, comments, and cyclic
// includes are ignored.
func parseLDSoConf(path string, maxDepth int) []string {
	visited := make(map[string]struct{})
	var dirs []string
	seenDirs := make(map[string]struct{})
	parseLDSoConfRec(path, maxDepth, visited, seenDirs, &dirs)
	return dirs
}

func parseLDSoConfRec(path string, depth int, visited, seenDirs map[string]struct{}, out *[]string) {
	if depth < 0 || path == "" || strings.IndexByte(path, 0) >= 0 {
		return
	}
	clean := filepath.Clean(path)
	if _, seen := visited[clean]; seen {
		return
	}
	visited[clean] = struct{}{}

	f, err := os.Open(clean)
	if err != nil {
		return
	}
	defer f.Close()

	baseDir := filepath.Dir(clean)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "include" {
			if depth == 0 {
				continue
			}
			for _, pattern := range fields[1:] {
				if strings.IndexByte(pattern, 0) >= 0 {
					continue
				}
				if !filepath.IsAbs(pattern) {
					pattern = filepath.Join(baseDir, pattern)
				}
				matches, globErr := filepath.Glob(pattern)
				if globErr != nil {
					continue
				}
				slices.Sort(matches)
				for _, m := range matches {
					parseLDSoConfRec(m, depth-1, visited, seenDirs, out)
				}
			}
			continue
		}
		dir := fields[0]
		if strings.IndexByte(dir, 0) >= 0 || !filepath.IsAbs(dir) {
			continue
		}
		dir = filepath.Clean(dir)
		if _, dup := seenDirs[dir]; dup {
			continue
		}
		seenDirs[dir] = struct{}{}
		*out = append(*out, dir)
	}
}

// expandSearchCandidates converts ordered search entries into concrete file
// candidate paths. Entries in explicitPaths may be either direct library file
// paths or directories; downloadDirs and extraDirs are expanded as directories
// against every name in libNames.
func expandSearchCandidates(explicitPaths, downloadDirs, extraDirs []string, libNames []string) []string {
	rejectUNC := runtime.GOOS == "windows"
	var candidates []string
	seenRaw := make(map[string]struct{})
	addCandidate := func(p string) {
		if p == "" || strings.IndexByte(p, 0) >= 0 {
			return
		}
		if rejectUNC && isUNCPath(p) {
			return
		}
		if !filepath.IsAbs(p) {
			return
		}
		p = filepath.Clean(p)
		if _, dup := seenRaw[p]; dup {
			return
		}
		seenRaw[p] = struct{}{}
		candidates = append(candidates, p)
	}
	expandDir := func(dir string) {
		if dir == "" || strings.IndexByte(dir, 0) >= 0 {
			return
		}
		if rejectUNC && isUNCPath(dir) {
			return
		}
		if !filepath.IsAbs(dir) {
			return
		}
		for _, name := range libNames {
			addCandidate(filepath.Join(dir, name))
		}
	}

	for _, raw := range explicitPaths {
		p := strings.TrimSpace(raw)
		if p == "" || strings.IndexByte(p, 0) >= 0 {
			continue
		}
		if rejectUNC && isUNCPath(p) {
			continue
		}
		if !filepath.IsAbs(p) {
			if abs, err := filepath.Abs(p); err == nil {
				p = abs
			} else {
				continue
			}
		}
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			addCandidate(p)
			continue
		}
		expandDir(p)
	}
	for _, dir := range downloadDirs {
		expandDir(dir)
	}
	for _, dir := range extraDirs {
		expandDir(dir)
	}
	return candidates
}
