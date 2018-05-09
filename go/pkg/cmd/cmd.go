package cmd

import (
	"flag"
	"fmt"
	"os"
)

// DieWithUsage is a utility that assumes usage of the flag library. It prints
// a usage line, the flag arguments, and then exits.
func DieWithUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}
