package main

import (
	"fmt"
	"io"
	"log"

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
	errorHandle io.Writer) {

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
}

func setExceptLogFile(brokenLog io.Writer, dbErrLog io.Writer) {
	BrokenLog = log.New(brokenLog, "", 0)

	DbErrLog = log.New(dbErrLog, "", 0)
}

// WriteDBLog 쿼리 오류가 발생하면, DB log 파일에 별도로 남긴다.
func WriteDBLog(err error, msg string) {

	logMsg := fmt.Sprintf("%s: %s", err, msg)
	if conf.EucKrLog {
		msgEucKr, _ := cp949.To([]byte(logMsg))
		Error.Println(string(msgEucKr))

		msgEucKr, _ = cp949.To([]byte(msg))
		DbErrLog.Println(string(msgEucKr) + ";")
	} else {
		Error.Println(logMsg)
		DbErrLog.Println(msg + ";")
	}
}

func WriteBrokenLog(query string) {

	if conf.EucKrLog {
		msgEucKr, _ := cp949.To([]byte(query))
		BrokenLog.Println(string(msgEucKr) + ";")
	} else {
		BrokenLog.Println(query + ";")
	}
}
