package main

import (
	"os"
	"os/exec"
)

// Windows has no exec(): run the JVM as a child and propagate its exit code.
func execJava(java string, args []string) error {
	cmd := exec.Command(java, args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		return err
	}
	os.Exit(0)
	return nil
}