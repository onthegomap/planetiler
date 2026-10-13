//go:build !windows

package main

import (
	"os"
	"syscall"
)

// Replaces this process with the JVM, so signals and exit codes pass straight through.
func execJava(java string, args []string) error {
	return syscall.Exec(java, args, os.Environ())
}