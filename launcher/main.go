package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// `payload` (gzipped tar) and `payloadID` come from the generated
// payload_<goos>_<goarch>.go file.

func main() {
	var memory string
	var jvmArgs, rest []string
	extractJar, verbose := false, false
	aot := "auto"

	// Launcher flags are intercepted anywhere and never reach planetiler, except --verbose, which both use.
	for _, a := range os.Args[1:] {
		switch {
		case a == "--extract-jar":
			extractJar = true
		case strings.HasPrefix(a, "--memory="):
			memory = unquote(strings.TrimPrefix(a, "--memory="))
		case strings.HasPrefix(a, "--jvm-args="):
			jvmArgs = append(jvmArgs, strings.Fields(unquote(strings.TrimPrefix(a, "--jvm-args=")))...)
		case strings.HasPrefix(a, "--aot="):
			aot = strings.TrimPrefix(a, "--aot=")
		default:
			verbose = verbose || a == "--verbose" || a == "--verbose=true"
			rest = append(rest, a)
		}
	}
	if aot != "auto" && aot != "off" && aot != "train" {
		check(fmt.Errorf("--aot must be auto, off or train, got %q", aot))
	}

	if extractJar {
		check(extract(".", func(name string) bool { return name == "planetiler.jar" }))
		fmt.Println("wrote planetiler.jar")
		return
	}

	cache := cacheDir()
	java := filepath.Join(cache, "runtime", "bin", "java")
	// Lets callers swap in a different Java launcher. A warm-up AOT cache trained on a different JVM only gets a warning and goes unused.
	if home := os.Getenv("PLANETILER_JAVA_HOME"); home != "" {
		java = filepath.Join(home, "bin", "java")
	}
	if runtime.GOOS == "windows" {
		java += ".exe"
	}
	check(ensureInstalled(cache))
	sweepOldInstalls(cache)
	unlock := lock(cache)
	jar := filepath.Join(cache, "planetiler.jar")
	launch := []string{"-jar", jar}
	aotCache := filepath.Join(cache, "planetiler.aot")
	if len(rest) > 0 && strings.HasSuffix(rest[0], ".java") {
		// Java source mode: compile and run the profile with planetiler on the classpath.
		cp := jar
		if extra := os.Getenv("PLANETILER_CLASSPATH"); extra != "" {
			cp += string(os.PathListSeparator) + extra
		}
		launch = []string{"-cp", cp}
		// The JVM rejects a cache trained with -jar in source mode, so these runs share their own. It holds the JDK,
		// compiler and planetiler classes but not the profile's, so training on one profile speeds up all of them.
		aotCache = filepath.Join(cache, "planetiler-java.aot")
	}
	// Concurrent trainers each write their own file and rename it into place, so no JVM ever sees a partial cache.
	trainingOutput := fmt.Sprintf("%s.tmp-%d", aotCache, os.Getpid())

	// Later flags win in the JVM: JAVA_OPTS < --memory < --jvm-args
	args := append([]string{java}, strings.Fields(os.Getenv("JAVA_OPTS"))...)
	if memory != "" {
		args = append(args, "-Xmx"+memory)
	}
	args = append(args, jvmArgs...)
	// Tells planetiler it runs under the launcher, and the name to show in help (`planetiler ...` instead of
	// `java -jar ...`).
	args = append(args, "-Dplanetiler.launcher="+filepath.Base(os.Args[0]))
	if verbose {
		args = append(args, "-showversion")
	}

	// `planetiler setup` trains the warm-up AOT cache on the Monaco test data bundled in the launcher, with no network
	// access, so later runs start faster. Rerunning it trains a new one.
	setup := len(rest) > 0 && rest[0] == "setup"
	workDir := ""
	if setup {
		fmt.Fprintln(os.Stderr, "planetiler is unpacked in", cache)
		// An empty working directory, so files the run writes relative to it don't end up next to the user's files
		var err error
		workDir, err = os.MkdirTemp("", "planetiler-setup-")
		check(err)
		data := filepath.Join(cache, "training")
		rest = []string{
			"--osm_path=" + filepath.Join(data, "monaco-latest.osm.pbf"),
			"--water_polygons_path=" + filepath.Join(data, "water_polygons.parquet"),
			"--lake_centerlines_path=" + filepath.Join(data, "lake_centerline.parquet"),
			"--natural_earth_path=" + filepath.Join(data, "natural_earth_shapefiles.zip"),
			"--output=" + filepath.Join(workDir, "monaco.pmtiles"),
			"--tmpdir=" + filepath.Join(workDir, "tmp"),
		}
		aot = "train"
	}

	switch {
	case aot == "train":
		fmt.Fprintln(os.Stderr, "Training the warm-up AOT cache...")
		// Hides the JVM's dozens of warnings about classes it leaves out of the cache (e.g. log4j's optional features).
		args = append(args, "-Xlog:aot=error", "-XX:AOTCacheOutput="+trainingOutput)
	case aot == "auto" && exists(aotCache):
		args = append(args, "-XX:AOTCache="+aotCache)
	}
	args = append(args, launch...)
	args = append(args, rest...)

	if verbose {
		fmt.Fprintln(os.Stderr, strings.Join(args, " "))
	}
	cmd := exec.Command(args[0], args[1:]...)
	var setupLog bytes.Buffer
	if setup {
		// The training run's own logs only matter when it fails.
		cmd.Dir, cmd.Stdout, cmd.Stderr = workDir, &setupLog, &setupLog
	} else {
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	}
	code := run(cmd)

	// Saving the cache is all `setup` is for, so it fails without one. A real run with --aot=train keeps its own result.
	if aot == "train" && !saveAotCache(code, trainingOutput, aotCache) && setup && code == 0 {
		code = 1
	}
	if setup {
		os.RemoveAll(workDir)
		if code != 0 {
			os.Stderr.Write(setupLog.Bytes())
		}
	}
	unlock()
	os.Exit(code)
}

// saveAotCache keeps a trained warm-up AOT cache only if the training run succeeded, since the JVM writes one even
// when the run fails early, trained on next to nothing. Returns whether it saved one.
func saveAotCache(exitCode int, trainingOutput, aotCache string) bool {
	if exitCode != 0 {
		os.Remove(trainingOutput)
		fmt.Fprintln(os.Stderr, "The run failed, so the warm-up AOT cache was not saved")
		return false
	}
	// On Windows the rename fails while another JVM has the old cache open. That one stays in use then.
	if err := os.Rename(trainingOutput, aotCache); err != nil {
		os.Remove(trainingOutput)
		fmt.Fprintln(os.Stderr, "Could not save the warm-up AOT cache:", err)
		return false
	}
	fmt.Fprintln(os.Stderr, "Saved the warm-up AOT cache to", aotCache)
	return true
}

// run runs cmd as a child process and returns its exit code. The launcher stays the parent instead of exec-ing the JVM
// so it can save a trained warm-up AOT cache and remove its lock file once the JVM exits.
func run(cmd *exec.Cmd) int {
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	// Send termination signals to the JVM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		for s := range signals {
			if s != syscall.SIGQUIT {
				cmd.Process.Signal(s)
			}
		}
	}()
	cmd.Wait() // a failed run shows up in the exit code
	// Like a shell, report a JVM killed by a signal as 128 + the signal number.
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal())
	}
	return cmd.ProcessState.ExitCode()
}

func unquote(s string) string { return strings.Trim(s, "\"'") }

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
