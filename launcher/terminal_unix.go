//go:build darwin || linux

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// terminalWidth returns the width of the terminal stdout goes to in columns, or 0 if stdout isn't a terminal.
func terminalWidth() int {
	// The layout of struct winsize from <sys/ioctl.h>, which the stdlib syscall package doesn't define.
	var size struct{ rows, cols, xpixel, ypixel uint16 }
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(), uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0
	}
	return int(size.cols)
}
