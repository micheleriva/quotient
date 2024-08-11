package main

import "flag"

var (
	ConfigFilePath = flag.String("config", DefaultConfigFilename, "Path to the configuration file")
)
