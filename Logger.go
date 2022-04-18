package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/suapapa/go_hangul/encoding/cp949"
)

// Trace log
var (
	Trace     *log.Logger
	Info      *log.Logger
	Warning   *log.Logger
	Error     *log.Logger
	BrokenLog *log.Logger
	DbErrLog  *log.Logger
)

type Logger struct {
	*log.Logger
}

// InitLogger 로거 초기화
func InitLogger(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) *os.File {

	Trace = log.New(traceHandle,
		"TRACE: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Warning = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	var sysLog *os.File
	if !isNoWriteLogFile {
		os.Mkdir("logs", 0755)
		sysLog = openFile("logs/sys_"+time.Now().Format("20060102")+".log", os.O_CREATE|os.O_APPEND|os.O_RDWR)

		multiWriter := io.MultiWriter(sysLog, os.Stdout)
		if enableTraceLog {
			Trace.SetOutput(multiWriter)
		} else {
			Trace.SetOutput(ioutil.Discard)
		}
		Info.SetOutput(multiWriter)
		Warning.SetOutput(multiWriter)

		multiWriter = io.MultiWriter(sysLog, os.Stderr)
		Error.SetOutput(multiWriter)
	}

	return sysLog
}

func setExceptLogFile(brokenLog io.Writer, dbErrLog io.Writer) {
	BrokenLog = log.New(brokenLog, "", 0)

	DbErrLog = log.New(dbErrLog, "", 0)
}

func getKrString(msg string) string {
	if conf.EucKrLog {
		msgEucKr, _ := cp949.To([]byte(msg))
		return string(msgEucKr)
	}

	return msg
}
