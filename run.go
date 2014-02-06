package main

import (
	"flag"
	"fmt"
	"github.com/spicavigo/gomr/master"
	"github.com/spicavigo/gomr/slave"
	"os"
)

func main() {
	var configFile = flag.String("config", "./config.json", "Configuration file")
	var port = flag.String("port", "1234", "Port for the RPC server")
	var ismaster = flag.Bool("master", false, "Run as master")
	var isslave = flag.Bool("slave", false, "Run as slave")

	flag.Parse()
	if !(*ismaster || *isslave) {
		fmt.Println("Use -master or -slave to specify the role for this server")
		os.Exit(1)
	}
	if *ismaster {
		master.Run(*configFile)
	}
	if *isslave {
		slave.Run(*configFile, *port)
	}
}
