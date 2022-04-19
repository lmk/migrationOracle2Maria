package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"github.com/suapapa/go_hangul/encoding/cp949"
)

// makeFieldName colName을 ``로 감싸고 ,로 연결한다
func makeFieldName(colNames []string) string {
	sep := ","
	switch len(colNames) {
	case 0:
		return ""
	case 1:
		return colNames[0]
	}
	n := len(sep)*(len(colNames)-1) + (len(colNames) * 2)
	for i := 0; i < len(colNames); i++ {
		n += len(colNames[i])
	}

	var b bytes.Buffer
	b.Grow(n)
	b.WriteString("`" + colNames[0] + "`")
	for _, s := range colNames[1:] {
		b.WriteString(sep)
		b.WriteString("`" + s + "`")
	}
	return b.String()
}

// makeInsertQuery 마리아DB용 insert 쿼리를 만든다.
func makeInsertQuery(table Table, values []interface{}, colNames []string, colInfo map[string]ColInfo) string {

	// 예외 컬럼
	skipList := make(map[string]int)
	for _, skipCol := range table.SkipColumns {
		skipList[skipCol] = 1
	}

	var textType = map[string]int{
		"LONG": 1, "CHAR": 1, "CLOB": 1, "VARCHAR2": 1,
	}

	valList := []string{}

	for i, val := range values {

		v := fmt.Sprintf("%v", val)

		if val == nil || len(v) == 0 || v == "<nil>" {
			// null 처리
			valList = append(valList, "null")
		} else if colInfo[colNames[i]].dataType == "DATE" {
			// DATE형은 YYYMMDDhh24miss로 받아서, DATETIME 형으로 넣는다.
			valList = append(valList, fmt.Sprintf("STR_TO_DATE('%s', '%%Y%%m%%d%%H%%i%%s')", v))
		} else if colInfo[colNames[i]].dataType == "RAW" {
			// RAW형은 HEX string으로 받아서 넣는다.
			valList = append(valList, "0x"+v)
		} else if textType[colInfo[colNames[i]].dataType] == 1 {
			// ' -> '' 로 변환
			v = strings.ReplaceAll(v, "'", "''")
			// \ -> \\ 로 변환
			v = strings.ReplaceAll(v, "\\", "\\\\")
			// text type은 ''로 감싼다
			valList = append(valList, "'"+v+"'")
		} else {
			valList = append(valList, v)
		}
	}

	return fmt.Sprintf("insert into %s (%s) values (%s)",
		table.TargetName,
		makeFieldName(colNames),
		strings.Join(valList, ","))
}

// ContainsNoEucKr euc-kr 범위 밖의 문자가 있으면 true를 반환 한다.
func ContainsNoEucKr(query string) bool {
	msgEucKr, _ := cp949.To([]byte(query))
	msgUtf8, _ := cp949.From(msgEucKr)

	return query != string(msgUtf8)
}

// RetryInsert retryQ를 읽어서 건별로 쿼리를 실행한다.
// 실패하면 별도 db 실패 로그 파일에 남긴다.
func RetryInsert(retryQ <-chan string, tableName string, tableState *TableStatus) {

	Trace.Printf("%s, start retry thread", tableName)

	maria := ConnectMaria(true)
	defer maria.Close()

RETRY:
	for {
		select {
		case msg := <-retryQ:
			Trace.Printf("%s, start retry child-thread %s", tableName, getKrString(msg))

			err := execQuery(maria, msg)

			if err != nil {
				logMsg := fmt.Sprintf("%s: %s", err, getKrString(msg))
				Error.Println(logMsg)
				DbErrLog.Println(msg + ";")

				tableState.dbErrorCount.Add(1)
			} else {
				Trace.Printf("%s, RETRY child-thread %s", tableName, getKrString(msg))

				tableState.retryCount.Add(1)
			}

			Trace.Printf("%s, end retry child-thread %s", tableName, getKrString(msg))

			continue

		case <-time.After(time.Second * 10): // 10초동안 q가 비어 있으면

			// insertThread가 있는지 체크해서 없으면 종료
			if tableState.threadCount.count <= 0 {
				break RETRY
			}
		}
	}

	maria.Close()

	Trace.Printf("%s, end retry thread", tableName)
	tableState.wait.Done()
}

// pushRetry list 전체를 retryQ에 넘긴다.
func pushRetry(threadIndex int, retryQ chan<- string, tableName string, list []string) {
	for _, msg := range list {
		if len(msg) > 0 {
			retryQ <- msg
			Trace.Printf("%s, %3d, push retryQ, %s", tableName, threadIndex, getKrString(msg))
		}
	}
}

// newInsert mariadb batch insert
// FetchSize당 트랜잭션 처리하고, 오류 발생시 rollback 하고, retryQ에 넘긴다.
func newInsert(threadIndex int, insertQ <-chan string, retryQ chan<- string, tableInfo Table, tableState *TableStatus) {

	Trace.Printf("%s, start thread %3d", tableInfo.TargetName, threadIndex)

	maria := ConnectMaria(false)
	defer maria.Close()

	// 트랜잭션 시작
	tx := startTransaction(maria)
	defer tx.Rollback()

	// 트랜잭션 실패시 재시도 쿼리를 저장할 버퍼
	buf := []string{}

	// msgQ를 읽어서
	for msg := range insertQ {
		//Trace.Printf("%s %03d, readQ, %s", tableInfo.TargetName, threadIndex, getKrString(msg))

		buf = append(buf, msg)

		_, err := tx.Exec(msg)
		if err != nil {

			Warning.Printf("%s, %3d, fail tx.Exec %s, %s", tableInfo.TargetName, threadIndex, err, getKrString(msg))

			err = tx.Rollback()
			if err != nil {
				Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.TargetName, threadIndex, err)
			}

			tx = startTransaction(maria)

			pushRetry(threadIndex, retryQ, tableInfo.TargetName, buf)

			buf = []string{}

		}

		// FetchSize 만큼
		if len(buf) >= tableInfo.FetchSize {
			//commit
			err = tx.Commit()
			if err != nil {
				Warning.Printf("%s, %3d, fail tx.commit %s", tableInfo.TargetName, threadIndex, err)

				err = tx.Rollback()
				if err != nil {
					Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.TargetName, threadIndex, err)
				}

				pushRetry(threadIndex, retryQ, tableInfo.TargetName, buf)
			}

			tx = startTransaction(maria)

			tableState.batchCount.Add(int64(len(buf)))

			buf = []string{}

			//Info.Printf("thread %d, commit %s", threadIndex, tableInfo.Name)
		}
	}

	// 남은 데이터 commit
	if len(buf) > 0 {
		//commit
		err := tx.Commit()
		if err != nil {
			Warning.Printf("%s, %3d, fail tx.commit %s", tableInfo.TargetName, threadIndex, err)

			err = tx.Rollback()
			if err != nil {
				Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.TargetName, threadIndex, err)
			}

			pushRetry(threadIndex, retryQ, tableInfo.TargetName, buf)

		} else {
			tableState.batchCount.Add(int64(len(buf)))
		}
	}

	maria.Close()
	Trace.Printf("%s, end thread %3d", tableInfo.TargetName, threadIndex)
	tableState.threadCount.Add(-1)
	tableState.wait.Done()
}

// makeSelectQuery 오라클 select 쿼리문을 만든다.
// DATE형은 문자열로 변경한다.
// RAW형은 hex 문자열로 변경한다.
func makeSelectQuery(tableInfo Table, colInfo map[string]ColInfo) string {

	fields := []string{}

	for key, col := range colInfo {

		if tableInfo.isSkipField(key) {
			continue
		}

		if col.dataType == "DATE" {
			fields = append(fields, fmt.Sprintf("TO_CHAR(%s, 'YYYYMMDDhh24miss') %s", key, key))
		} else if col.dataType == "RAW" {
			fields = append(fields, fmt.Sprintf("RAWTOHEX(%s) %s", key, key))
		} else {
			fields = append(fields, key)
		}
	}

	return fmt.Sprintf("select %s from %s", strings.Join(fields, ","), tableInfo.SourceName)
}

// newSelect 오라클에서 select 해서 마리아용 insert 문을 만들어서, insertQ에 넣는다.
func newSelect(insertQ chan<- string, tableInfo Table, status *TableStatus) {

	Trace.Printf("%s, start select thread", tableInfo.SourceName)
	count := 0

	ora := ConnectOracle()
	defer ora.Close()

	// 오라클 컬럼 정보를 읽는다.
	colInfo, err := getColumnInfo(ora, tableInfo.SourceName)
	if err != nil {
		Error.Fatal(err)
	}
	//Info.Printf("%s Columns: %v", tableInfo.Name, colInfo)

	// 전체 데이터 조회
	query := makeSelectQuery(tableInfo, colInfo)
	rows, err := ora.Query(query)
	if err != nil {
		Error.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		Error.Fatal(err)
	}
	defer rows.Close()

	// insert 쿼리문 생성
	for rows.Next() {
		values := make([]interface{}, len(colInfo))
		pointers := make([]interface{}, len(colInfo))
		for i := range values {
			pointers[i] = &values[i]
		}
		err = rows.Scan(pointers...)
		if err != nil {
			Error.Fatal(err)
		}

		query = makeInsertQuery(tableInfo, values, columns, colInfo)

		// 만든 쿼리를 큐에 넣는다.
		// euckr 범위 밖 문자가 있으면 broken log에 남기고, 실행하지 않는다.
		if conf.CheckEucKr && ContainsNoEucKr(query) {
			BrokenLog.Println(getKrString(query + ";"))
			status.brokenCount.Add(1)
			count++
		} else {
			insertQ <- query
		}
	}

	close(insertQ)

	Trace.Printf("%s, end select thread %d", tableInfo.SourceName, count)
	status.wait.Done()
}

// migrationTable 테이블 하나를 마이그레이션 한다.
func migrationTable(tableInfo Table) Report {

	var status TableStatus

	insertQ := make(chan string, 1000)
	retryQ := make(chan string, 1000)

	status.threadCount.Add(tableInfo.ThreadCount)

	// maria retry thread 생성
	status.wait.Add(1)
	go RetryInsert(retryQ, tableInfo.TargetName, &status)

	// thread 개수만큼 maria insert thread 생성
	threadCount := tableInfo.ThreadCount

	for threadCount > 0 {
		status.wait.Add(1)
		go newInsert(threadCount, insertQ, retryQ, tableInfo, &status)
		threadCount--
	}

	// oracle select thread
	status.wait.Add(1)
	go newSelect(insertQ, tableInfo, &status)

	status.wait.Wait()

	close(retryQ)

	return status.ToReport()
}

// migrationAndReport 테이블 하나를 마이그레이션 하고 리포팅 한다.
func migrationAndReport(tableInfo Table) {
	startTime := time.Now()

	if strings.EqualFold(tableInfo.BeforeTruncate, "true") {
		truncateTable(tableInfo.TargetName)
		Info.Printf("%s, truncate table", tableInfo.TargetName)
	}

	report := migrationTable(tableInfo)

	duration := time.Since(startTime)
	reportTable(tableInfo, &report, duration)
}

// migrationAndReportMulti src 테이블이 복수개인 경우,
// 테이블 여러개를 마이그레이션 하고 리포팅 한다.
func migrationAndReportMulti(tableInfo Table) {

	startTime := time.Now()

	var waitGroup sync.WaitGroup

	tableList := []Table{}
	reportList := []Report{}

	// src 테이블이 복수개인 경우 한번만 truncate 한다.
	if strings.EqualFold(tableInfo.BeforeTruncate, "true") {
		truncateTable(tableInfo.TargetName)
		Info.Printf("%s, truncate table", tableInfo.TargetName)
	}

	fmtString := tableInfo.SourceName

	for i := tableInfo.StartIndex; i <= tableInfo.EndIndex; i++ {

		tableInfo.SourceName = fmt.Sprintf(fmtString, i)
		if strings.Contains(tableInfo.SourceName, "!") {
			Error.Fatalf("Invalid SourceName: %s", fmtString)
		}

		tableList = append(tableList, tableInfo)

		waitGroup.Add(1)
		go func(tableInfo Table) {
			report := migrationTable(tableInfo)
			reportList = append(reportList, report)
			waitGroup.Done()
		}(tableInfo)
	}

	// group thread가 모두 끝나야 report를 한다.
	waitGroup.Wait()

	duration := time.Since(startTime)
	reportMultiTables(tableList, reportList, duration)
}
