package main

// terminalWidth returns 0 (unknown) on Windows, where planetiler then wraps its help at a fixed width.
func terminalWidth() int {
	return 0
}
