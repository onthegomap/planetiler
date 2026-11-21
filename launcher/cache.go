package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// The payload unpacks into its own directory per build (payloadID), under PLANETILER_CACHE_DIR or the user's cache
// directory. It holds the Java runtime, planetiler.jar, the warm-up AOT cache and a lock file per running launcher.

func cacheDir() string {
	if dir := os.Getenv("PLANETILER_CACHE_DIR"); dir != "" {
		// absolute, since `setup` runs java from another working directory
		dir, err := filepath.Abs(dir)
		check(err)
		return filepath.Join(dir, payloadID)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "planetiler", payloadID)
}

// ensureInstalled unpacks the payload into cache unless it's already there.
func ensureInstalled(cache string) error {
	// The unpack is renamed into place as a whole, so the jar being there means everything else is too.
	jar := filepath.Join(cache, "planetiler.jar")
	if exists(jar) {
		return nil
	}
	parent := filepath.Dir(cache)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, payloadID+".tmp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp) // no-op after a successful rename
	if err := extract(tmp, nil); err != nil {
		return err
	}
	os.RemoveAll(cache) // broken/partial install, if any
	if err := os.Rename(tmp, cache); err != nil {
		if exists(jar) {
			return nil // another process won the race
		}
		return err
	}
	return nil
}

// lock marks cache as in use until the returned function is called, so other launchers' sweeps leave it alone.
func lock(cache string) (unlock func()) {
	path := filepath.Join(cache, fmt.Sprintf("running-%d.lock", os.Getpid()))
	os.WriteFile(path, nil, 0o644)
	// Long runs refresh it, so a lock that's more than a day old was left by a launcher that got killed.
	go func() {
		for range time.Tick(time.Hour) {
			now := time.Now()
			os.Chtimes(path, now, now)
		}
	}()
	return func() { os.Remove(path) }
}

// <payloadID> or <payloadID>.tmp-<random> (an unpack in progress, or one that was interrupted)
var unpackedDir = regexp.MustCompile(`^[0-9a-f]{12}(\.tmp-.*)?$`)

// sweepOldInstalls removes the directories other launcher builds unpacked next to cache, unless one is in use, plus leftovers in
// cache from launchers that were killed. Errors are ignored: whatever can't be removed now (e.g. a file another
// process has open on Windows) goes on a later run.
func sweepOldInstalls(cache string) {
	parent := filepath.Dir(cache)
	entries, _ := os.ReadDir(parent)
	for _, e := range entries {
		dir := filepath.Join(parent, e.Name())
		// PLANETILER_CACHE_DIR may hold other files, so a directory named like an unpacked one must also contain
		// planetiler.jar. Interrupted unpacks may not have it yet, and their name is specific enough on its own.
		ours := unpackedDir.MatchString(e.Name()) &&
			(strings.Contains(e.Name(), ".tmp-") || exists(filepath.Join(dir, "planetiler.jar")))
		if dir != cache && e.IsDir() && ours && !inUse(dir) {
			os.RemoveAll(dir)
		}
	}
	removeOlderThan(filepath.Join(cache, "running-*.lock"), 24*time.Hour)
	removeOlderThan(filepath.Join(cache, "*.aot.tmp-*"), 24*time.Hour)
}

// inUse reports whether a launcher holds a lock in dir, or dir changed in the last hour (being unpacked right now).
func inUse(dir string) bool {
	if modifiedWithin(dir, time.Hour) {
		return true
	}
	locks, _ := filepath.Glob(filepath.Join(dir, "running-*.lock"))
	for _, l := range locks {
		if modifiedWithin(l, 24*time.Hour) {
			return true
		}
	}
	return false
}

func removeOlderThan(pattern string, age time.Duration) {
	files, _ := filepath.Glob(pattern)
	for _, f := range files {
		if !modifiedWithin(f, age) {
			os.Remove(f)
		}
	}
}

func modifiedWithin(path string, age time.Duration) bool {
	info, err := os.Stat(path)
	return err == nil && time.Since(info.ModTime()) < age
}

// extract unpacks the payload into dst; if keep != nil, only files it accepts.
func extract(dst string, keep func(name string) bool) error {
	gz, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if h.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		name := filepath.FromSlash(strings.TrimPrefix(h.Name, "./"))
		if name == "" || name == "." {
			continue
		}
		if !filepath.IsLocal(name) {
			return fmt.Errorf("unsafe path in payload: %q", h.Name)
		}
		if keep != nil && !keep(h.Name) {
			continue
		}
		target := filepath.Join(dst, name)

		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				err = writeFile(target, tr, h.FileInfo().Mode().Perm())
			}
			if err == nil {
				// The JVM only uses a warm-up AOT cache with the jar it was trained on, which it checks by mtime.
				err = os.Chtimes(target, h.ModTime, h.ModTime)
			}
		case tar.TypeSymlink:
			if runtime.GOOS == "windows" {
				continue // needs privileges; only used for duplicate files
			}
			if err = os.MkdirAll(filepath.Dir(target), 0o755); err == nil {
				err = os.Symlink(h.Linkname, target)
			}
		default:
			err = fmt.Errorf("unsupported entry type %q in payload: %s", h.Typeflag, h.Name)
		}
		if err != nil {
			return err
		}
	}
}

func writeFile(path string, r io.Reader, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
