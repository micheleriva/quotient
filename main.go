package main

import (
	"flag"
	"fmt"
)

var (
	Configuration *Config
	QF            *QuotientFilter
)

func init() {
	flag.Parse()

	config, err := ParseConfigFile(*ConfigFilePath)
	if err != nil {
		fmt.Println(err)
	}

	Configuration = config
	QF = NewQuotientFilter(config.Quotient.LogSize)
}

func main() {
	StartServer(Configuration)
}
