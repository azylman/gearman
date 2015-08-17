package main

import (
	"flag"
	"fmt"
	"os"
)

func fatal(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

func main() {
	port := flag.String("p", "4731", "job server port")
	server := NewServer()
	if err := server.Listen(":" + *port); err != nil {
		fatal("failed to listen: %s", err.Error())
	}
}
