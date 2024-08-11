package main

import (
	"flag"
	"fmt"
)

func main() {
	flag.Parse()

	config, err := ParseConfigFile(*ConfigFilePath)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(config)
}
