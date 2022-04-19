package main

import (
	"fmt"
	"os"
	"strings"
)

var BUILDDT string
var VERSION string

func showVersion() {

	if len(os.Args) > 1 {
		arg := os.Args[1]
		if strings.Compare(arg, "-v") == 0 ||
			strings.Compare(arg, "version") == 0 ||
			strings.Compare(arg, "--version") == 0 ||
			strings.Compare(arg, "-version") == 0 {
			fmt.Printf("Version: %s\n", VERSION)
			fmt.Printf("Build Date: %s\n", BUILDDT)

			os.Exit(0)
		}
	}
}
