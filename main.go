package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var conf AppConfig // yamlFile에서 읽은 설정

var yamlFile string       // args 에서 읽은 .yml 파일명
var isNoWriteLogFile bool // args 에서 읽은 로그 파일로 쓸지 여부

var waitGlobal sync.WaitGroup

func usage() {
	fmt.Printf("Usage: %s -config=\"yaml file\" [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func openFile(filename string, flag int) *os.File {

	logFile, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		panic(err)
	}

	return logFile
}

func init() {
	flag.StringVar(&yamlFile, "config", "config.yml", "config yaml file name(.yml)")
	flag.BoolVar(&isNoWriteLogFile, "nolog", false, "if true, NO file log is written. only stdio")
	flag.Usage = usage
}

func getConfigName() string {

	if _, err := os.Stat(yamlFile); os.IsNotExist(err) {
		usage()
		Error.Printf("not exists file: %s", yamlFile)
		return "config.yml"
	}

	return yamlFile
}

func main() {

	flag.Parse()

	// setting log
	InitLogger(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	var sysLog *os.File
	if !isNoWriteLogFile {
		os.Mkdir("logs", 0755)
		sysLog = openFile("logs/sys_"+time.Now().Format("20060102")+".log", os.O_CREATE|os.O_APPEND|os.O_RDWR)

		multiWriter := io.MultiWriter(sysLog, os.Stdout)
		Info.SetOutput(multiWriter)
		Warning.SetOutput(multiWriter)

		multiWriter = io.MultiWriter(sysLog, os.Stderr)
		Error.SetOutput(multiWriter)
	}
	defer sysLog.Close()

	// read config
	err := conf.readConfig(getConfigName())
	if err != nil {
		Info.Fatal(err)
	}

	err = conf.checkRequired()
	if err != nil {
		Info.Fatal(err)
	}

	Info.Printf("%+v\n", makePretty(&conf))

	// euckr아닌 sql
	brokenLog := openFile(conf.BrokenLog, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	defer brokenLog.Close()

	// db 에러가 난 경우 sql
	dbErrLog := openFile(conf.DbErrLog, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	defer dbErrLog.Close()

	setExceptLogFile(brokenLog, dbErrLog)

	startTime := time.Now()

	for _, tableInfo := range conf.Tables {
		waitGlobal.Add(1)
		go func(tableInfo Table) {
			migrationTable(tableInfo)
			waitGlobal.Done()
		}(tableInfo)
	}

	waitGlobal.Wait()

	duration := time.Since(startTime)
	Info.Printf("%d Tables Duration %v", len(conf.Tables), duration)

	Info.Println("End Job.")
}
