package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// `payload` (gzipped tar) and `payloadID` come from the generated
// payload_<goos>_<goarch>.go file.

// Always passed to the JVM, before user-supplied args.
var defaultJVMArgs = []string{}

func main() {
	var (
		memory     string
		jvmArgs    []string
		extractJar bool
		rest       []string
	)
	// Launcher flags are intercepted anywhere and never reach planetiler.
	for _, a := range os.Args[1:] {
		switch {
		case a == "--extract-jar":
			extractJar = true
		case strings.HasPrefix(a, "--memory="):
			memory = unquote(strings.TrimPrefix(a, "--memory="))
		case strings.HasPrefix(a, "--jvm-args="):
			jvmArgs = append(jvmArgs, strings.Fields(unquote(strings.TrimPrefix(a, "--jvm-args=")))...)
		default:
			rest = append(rest, a)
		}
	}

	if extractJar {
		if err := extractJarTo("planetiler.jar"); err != nil {
			fail(err)
		}
		fmt.Println("wrote planetiler.jar")
		return
	}

	cache := cacheDir()
	javaName := "java"
	if runtime.GOOS == "windows" {
		javaName = "java.exe"
	}
	java := filepath.Join(cache, "runtime", "bin", javaName)
	if err := ensureInstalled(cache, java); err != nil {
		fail(err)
	}
	jar := filepath.Join(cache, "planetiler.jar")

	// Later flags win in the JVM: JAVA_OPTS < --memory < --jvm-args
	args := []string{java}
	args = append(args, defaultJVMArgs...)
	args = append(args, strings.Fields(os.Getenv("JAVA_OPTS"))...)
	if memory != "" {
		args = append(args, "-Xmx"+memory)
	}
	args = append(args, jvmArgs...)

	if len(rest) > 0 && strings.HasSuffix(rest[0], ".java") {
		// single-file source mode: compile and run with planetiler on the classpath
		cp := jar
		if extra := os.Getenv("PLANETILER_CLASSPATH"); extra != "" {
			cp += string(os.PathListSeparator) + extra
		}
		args = append(args, "-cp", cp)
	} else {
		args = append(args, "-jar", jar)
	}
	args = append(args, rest...)

	fail(execJava(java, args)) // only returns on error
}

func unquote(s string) string { return strings.Trim(s, "\"'") }

func cacheDir() string {
	if home := os.Getenv("PLANETILER_HOME"); home != "" {
		return filepath.Join(home, payloadID)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "planetiler", payloadID)
}

func ensureInstalled(cache, java string) error {
	if _, err := os.Stat(java); err == nil {
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
	if err := extractAll(tmp); err != nil {
		os.RemoveAll(tmp)
		return err
	}
	os.RemoveAll(cache) // broken/partial install, if any
	if err := os.Rename(tmp, cache); err != nil {
		os.RemoveAll(tmp)
		if _, e := os.Stat(java); e == nil {
			return nil // another process won the race
		}
		return err
	}
	return nil
}

func walkTar(fn func(*tar.Header, io.Reader) (stop bool, err error)) error {
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
		stop, err := fn(h, tr)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
}

func extractAll(dst string) error {
	root := filepath.Clean(dst) + string(os.PathSeparator)
	return walkTar(func(h *tar.Header, r io.Reader) (bool, error) {
		target := filepath.Join(dst, filepath.FromSlash(h.Name))
		if !strings.HasPrefix(target+string(os.PathSeparator), root) {
			return false, fmt.Errorf("unsafe path in payload: %q", h.Name)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			return false, os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			mode := h.FileInfo().Mode().Perm()
			if strings.HasPrefix(h.Name, "runtime/bin/") || strings.HasSuffix(h.Name, "/jspawnhelper") {
				mode |= 0o111
			}
			return false, writeFile(target, r, mode)
		case tar.TypeSymlink:
			if runtime.GOOS == "windows" {
				return false, nil // needs privileges; only used for duplicate legal notices
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return false, err
			}
			return false, os.Symlink(h.Linkname, target)
		case tar.TypeLink:
			src := filepath.Join(dst, filepath.FromSlash(h.Linkname))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return false, err
			}
			if err := os.Link(src, target); err != nil {
				return false, copyFile(src, target)
			}
		}
		return false, nil
	})
}

func extractJarTo(dst string) error {
	found := false
	err := walkTar(func(h *tar.Header, r io.Reader) (bool, error) {
		if h.Typeflag == tar.TypeReg && strings.TrimPrefix(h.Name, "./") == "planetiler.jar" {
			found = true
			return true, writeFile(dst, r, 0o644)
		}
		return false, nil
	})
	if err == nil && !found {
		err = fmt.Errorf("planetiler.jar not found in payload")
	}
	return err
}

func writeFile(path string, r io.Reader, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
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

func copyFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	return writeFile(dst, f, st.Mode().Perm())
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "planetiler:", err)
		os.Exit(1)
	}
}
