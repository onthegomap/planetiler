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

func main() {
	var memory string
	var jvmArgs, rest []string
	extractJar := false

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
		check(extract(".", func(name string) bool { return name == "planetiler.jar" }))
		fmt.Println("wrote planetiler.jar")
		return
	}

	cache := cacheDir()
	java := filepath.Join(cache, "runtime", "bin", "java")
	if runtime.GOOS == "windows" {
		java += ".exe"
	}
	check(ensureInstalled(cache, java))
	jar := filepath.Join(cache, "planetiler.jar")

	// Later flags win in the JVM: JAVA_OPTS < --memory < --jvm-args
	args := append([]string{java, "-showversion"}, strings.Fields(os.Getenv("JAVA_OPTS"))...)
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

	check(execJava(java, append(args, rest...))) // only returns on error
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
	defer os.RemoveAll(tmp) // no-op after a successful rename
	if err := extract(tmp, nil); err != nil {
		return err
	}
	os.RemoveAll(cache) // broken/partial install, if any
	if err := os.Rename(tmp, cache); err != nil {
		if _, e := os.Stat(java); e == nil {
			return nil // another process won the race
		}
		return err
	}
	return nil
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

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "planetiler:", err)
		os.Exit(1)
	}
}
