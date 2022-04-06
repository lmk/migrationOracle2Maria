package main

import (
	"io/ioutil"
	"os"
	"sync"
)

var conf AppConfig

// table list
var wait sync.WaitGroup

func main() {

	InitLogger(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	// read config
	Info.Println("Parsing YAML file")

	c, err := readConfig("config.yml")
	if err != nil {
		Info.Fatal(err)
	}

	conf = *c

	Info.Printf("%+v\n", makePretty(&conf))

	for _, tableInfo := range conf.Tables {
		wait.Add(1)
		go func(tableInfo Table) {
			migrationTable(tableInfo)
			wait.Done()
		}(tableInfo)
	}

	wait.Wait()

	Info.Println("End Job.")
}
