package main

import (
	"fmt"
)

var (
	Configuration *Config
	QF            *QuotientFilter
)

func init() {
	config, err := ParseConfigFile()
	if err != nil {
		fmt.Println(err)
	}

	Configuration = config
	QF = NewQuotientFilter(config.Quotient.LogSize)
}

func main() {
	StartServer(Configuration)
}
