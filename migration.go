package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
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

// makeInsertQuery
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
		table.Name,
		makeFieldName(colNames),
		strings.Join(valList, ","))
}

// ContainsNoEucKr euc-kr 범위 밖의 문자가 있으면 true를 반환 한다.
func ContainsNoEucKr(query string) bool {
	msgEucKr, _ := cp949.To([]byte(query))
	msgUtf8, _ := cp949.From(msgEucKr)

	return query != string(msgUtf8)
}

// RetryInsert
func RetryInsert(retryQ <-chan string, tableName string, tableState *TableStatus) {

	Trace.Printf("%s, start retry thread", tableName)

	maria := ConnectMaria()
	defer maria.Close()

RETRY:
	for {
		select {
		case msg := <-retryQ:

			Trace.Printf("%s, start retry child-thread %s", tableName, getKrString(msg))

			err := execQuery(maria, msg)

			if err != nil {
				logMsg := fmt.Sprintf("%s: %s", err, msg)
				Error.Println(getKrString(logMsg))
				DbErrLog.Println(getKrString(msg + ";"))

				tableState.dbErrorCount.Add(1)
			} else {
				Trace.Printf("%s, RETRY child-thread %s", tableName, getKrString(msg))

				tableState.retryCount.Add(1)
			}

			Trace.Printf("%s, end retry child-thread", tableName)

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

// newInsert mariadb batch insert
func newInsert(threadIndex int, insertQ <-chan string, retryQ chan<- string, tableInfo Table, tableState *TableStatus) {

	Trace.Printf("%s, start thread %3d", tableInfo.Name, threadIndex)

	maria := ConnectMaria()
	defer maria.Close()

	// 트랜잭션 시작
	count := 0
	tx, err := maria.Begin()
	if err != nil {
		Error.Fatal(err)
	}
	defer tx.Rollback()

	// 트랜잭션 실패시 재시도 쿼리를 저장할 버퍼
	buf := []string{}

	// msgQ를 읽어서
	for msg := range insertQ {
		_, err := tx.Exec(msg)
		if err != nil {
			retryQ <- msg
			Trace.Printf("%s, %3d, push retryQ, %s, %s", tableInfo.Name, threadIndex, err, getKrString(msg))
			continue
		} else {
			buf = append(buf, msg)
			count++
		}

		// FetchSize 만큼
		if count >= tableInfo.FetchSize {
			//commit
			err = tx.Commit()
			if err != nil {
				Warning.Printf("%s, fail tx.commit %s", tableInfo.Name, err)
				for _, m := range buf {
					if len(m) > 0 {
						retryQ <- m
						Trace.Printf("%s, %3d, push retryQ, %s", tableInfo.Name, threadIndex, getKrString(msg))
					}
				}

				err = tx.Rollback()
				if err != nil {
					Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.Name, threadIndex, err)
				}

				count = 0
			}

			buf = []string{}

			tx, err = maria.Begin()
			if err != nil {
				Error.Fatal(err)
			}

			tableState.batchCount.Add(int64(count))
			count = 0
			//Info.Printf("thread %d, commit %s", threadIndex, tableInfo.Name)
		}
	}

	// 남은 데이터 commit
	if count > 0 {
		//commit
		err = tx.Commit()
		if err != nil {
			Warning.Printf("fail tx %s", err)
			for _, msg := range buf {
				if len(msg) > 0 {
					retryQ <- msg
					Trace.Printf("%s, %3d, push retryQ, %s", tableInfo.Name, threadIndex, getKrString(msg))
				}
			}

			err = tx.Rollback()
			if err != nil {
				Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.Name, threadIndex, err)
			}
		} else {
			tableState.batchCount.Add(int64(count))
		}
	}

	maria.Close()
	Trace.Printf("%s, end thread %3d", tableInfo.Name, threadIndex)
	tableState.threadCount.Add(-1)
	tableState.wait.Done()
}

func reportTable(tableInfo Table, status *TableStatus, ora *sql.DB, duration time.Duration) {

	// 오라클 count
	err := ora.QueryRow("select count(*) from " + tableInfo.Name).Scan(&status.oracleRow)
	if err != nil {
		Error.Fatal(err)
	}

	// 마리아 count
	maria := ConnectMaria()
	defer maria.Close()

	err = maria.QueryRow("select count(*) from " + tableInfo.Name).Scan(&status.mariaRow)
	if err != nil {
		Error.Fatal(err)
	}

	// 로그 출력
	Info.Printf("%s, Report Oracle:%d, Maria:%d, broken:%d, dbError:%d, batch:%d, retry:%d, duration:%v",
		tableInfo.Name,
		status.oracleRow,
		status.mariaRow,
		status.brokenCount.count,
		status.dbErrorCount.count,
		status.batchCount.count,
		status.retryCount.count,
		duration)

	if status.oracleRow != (status.mariaRow + status.brokenCount.count + status.dbErrorCount.count) {
		Error.Printf("%s, miss count oracle", tableInfo.Name)
	}

	if status.mariaRow != (status.batchCount.count + status.retryCount.count) {
		Error.Printf("%s, miss count maria", tableInfo.Name)
	}
}

func (t *Table) isSkipField(fieldName string) bool {

	for _, f := range t.SkipColumns {
		if fieldName == f {
			return true
		}
	}

	return false
}

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

	return fmt.Sprintf("select %s from %s", strings.Join(fields, ","), tableInfo.Name)
}

func newSelect(insertQ chan<- string, ora *sql.DB, tableInfo Table, status *TableStatus) {

	Trace.Printf("%s, start select thread", tableInfo.Name)

	// 오라클 컬럼 정보를 읽는다.
	colInfo, err := getColumnInfo(ora, tableInfo.Name)
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
		} else {
			insertQ <- query
		}
	}

	close(insertQ)

	Trace.Printf("%s, end select thread", tableInfo.Name)
	status.wait.Done()
}

func migrationTable(tableInfo Table) {

	startTime := time.Now()

	if strings.EqualFold(tableInfo.BeforeTruncate, "true") {
		truncateTable(tableInfo.Name)
		Info.Printf("%s, truncate table", tableInfo.Name)
	}

	insertQ := make(chan string, 1000)
	retryQ := make(chan string, 1000)

	var status TableStatus
	status.threadCount.Add(tableInfo.ThreadCount)

	// maria retry thread 생성
	status.wait.Add(1)
	go RetryInsert(retryQ, tableInfo.Name, &status)

	// thread 개수만큼 maria insert thread 생성
	threadCount := tableInfo.ThreadCount

	for threadCount > 0 {
		status.wait.Add(1)
		go newInsert(threadCount, insertQ, retryQ, tableInfo, &status)
		threadCount--
	}

	// 오라클에 접속
	ora, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", conf.Oracle.User, conf.Oracle.Password, conf.Oracle.Database))
	if err != nil {
		Error.Fatal(err)
	}
	defer ora.Close()

	// oracle select thread
	status.wait.Add(1)
	go newSelect(insertQ, ora, tableInfo, &status)

	// 모든 thread가 종료되면, 리포팅
	status.wait.Wait()

	close(retryQ)

	duration := time.Since(startTime)
	reportTable(tableInfo, &status, ora, duration)
}
