package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"github.com/suapapa/go_hangul/encoding/cp949"
)

type ColInfo struct {
	dataType   string
	dataLength int
}

// getColumnInfo 오라클 컬럼 목록을 읽는다.
func getColumnInfo(ora *sql.DB, tableName string) (map[string]ColInfo, error) {

	// 컬럼 정보 조회
	colInfoList := make(map[string]ColInfo)

	query := fmt.Sprintf("select column_name, data_type, data_length from dba_tab_columns where table_name='%s' order by column_id", tableName)
	rows, err := ora.Query(query)
	if err != nil {
		return colInfoList, err
	}
	defer rows.Close()

	for rows.Next() {
		var colInfo ColInfo
		var colName string
		rows.Scan(&colName, &colInfo.dataType, &colInfo.dataLength)
		colInfoList[colName] = colInfo

		//Info.Printf("The data is: %s, %s, %v\n", tableInfo.Name, colName, colInfo)
	}

	return colInfoList, nil
}

// makeInsertQuery
// TODO 아래 자료형 테스트 해야함,
//  date
//  raw
func makeInsertQuery(table Table, values []interface{}, colNames []string, colInfo map[string]ColInfo) string {

	// 예외 컬럼
	skipList := make(map[string]int)
	for _, skipCol := range table.SkipColumns {
		skipList[skipCol] = 1
	}

	var textType = map[string]int{
		"LONG": 1, "CHAR": 1, "CLOB": 1, "VARCHAR2": 1,
	}

	valList := make([]string, len(colNames))

	for i, val := range values {

		// SkipColumns
		if skipList[colNames[i]] == 1 {
			continue
		}

		v := fmt.Sprintf("%v", val)

		if len(v) == 0 {
			valList[i] = "null"
		} else if textType[colInfo[colNames[i]].dataType] == 1 {
			valList[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		} else {
			valList[i] = v
		}
	}

	return fmt.Sprintf("insert into %s (%s) values (%s)", table.Name, strings.Join(colNames, ","), strings.Join(valList, ","))
}

func ConnectMaria() *sql.DB {
	maria, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.Maria.User, conf.Maria.Password, conf.Maria.Ip, conf.Maria.Port, conf.Maria.Database))
	if err != nil {
		Error.Fatal(err)
	}

	return maria
}

// truncateTable mariadb 테이블을 truncate 한다.
func truncateTable(tableName string) {

	maria := ConnectMaria()
	defer maria.Close()

	_, err := maria.Exec(fmt.Sprintf("truncate table %s", tableName))
	if err != nil {
		Error.Fatal(err)
	}

	Info.Printf("truncate table %s", tableName)
}

func migrationTable(tableInfo Table) {

	if strings.EqualFold(tableInfo.BeforeTruncate, "true") {
		truncateTable(tableInfo.Name)
	}

	msgQ := make(chan string, 1000)
	retryQ := make(chan string, 1000)

	// retry thread
	wait.Add(1)
	go func(msgQ chan string) {
		Info.Printf("thread retry, start %s", tableInfo.Name)
		maria := ConnectMaria()
		defer maria.Close()

		msg := ""
		sleepCount := 0

		for {

			select {
			case msg = <-msgQ:
				_, err := maria.Exec(msg)
				if err != nil {
					msgEucKr, _ := cp949.To([]byte(fmt.Sprintf("%s: %s", err, msg)))
					Error.Println(string(msgEucKr))
				}
				sleepCount = 0

			case <-time.After(time.Second * 3): // 3초동안 q가 비어 있으면 1초 쉬고 대기
				time.Sleep(1000 * time.Millisecond)
				sleepCount++

				// 60초 이상 쉬고 있으면 retry thread 종료
				if sleepCount > 20 {
					wait.Done()
					Info.Printf("thread retry, end %s", tableInfo.Name)
					return
				}

				continue
			}

		}
	}(retryQ)

	// thread 개수만큼 데이터를 나눈다.
	threadCount := tableInfo.ThreadCount

	for threadCount > 0 {
		wait.Add(1)
		Info.Printf("thread %d, start %s", threadCount, tableInfo.Name)
		go func(threadIndex int, msgQ chan string) {

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
			buf := make([]string, tableInfo.FetchSize)
			msg := ""

			// msgQ를 읽어서
			for {
				select {
				case msg = <-msgQ:
					//Info.Printf("thread %d, %s", threadCount, msg)
					buf[count] = msg
					_, err := tx.Exec(msg)
					if err != nil {
						retryQ <- msg
					}
					count++

					// FetchSize 만큼
					if count >= tableInfo.FetchSize {
						//commit
						err = tx.Commit()
						if err != nil {
							Warning.Printf("fail tx %s", err)
							for _, m := range buf {
								if len(m) > 0 {
									retryQ <- m
								}
							}
						}

						buf = make([]string, tableInfo.FetchSize)

						tx, err = maria.Begin()
						if err != nil {
							Error.Fatal(err)
						}

						count = 0
						//Info.Printf("thread %d, commit %s", threadIndex, tableInfo.Name)
					}

				case <-time.After(time.Second * 10): // 10초동안 q가 비어 있으면 종료

					if count > 0 {
						//commit
						err = tx.Commit()
						if err != nil {
							Warning.Printf("fail tx %s", err)
							for _, m := range buf {
								if len(m) > 0 {
									retryQ <- m
								}
							}
						}

						//Info.Printf("thread %d, no data commit %s", threadIndex, tableInfo.Name)
					}

					Info.Printf("thread %d, end %s", threadIndex, tableInfo.Name)
					wait.Done()
					return
				}
			}
		}(threadCount, msgQ)
		threadCount--
	}

	// 오라클에 접속한다.
	ora, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", conf.Oracle.User, conf.Oracle.Password, conf.Oracle.Database))
	if err != nil {
		Error.Fatal(err)
	}
	defer ora.Close()

	// 오라클 컬럼 정보를 읽는다.
	colInfo, err := getColumnInfo(ora, tableInfo.Name)
	if err != nil {
		Error.Fatal(err)
	}
	Info.Printf("%s Columns: %v", tableInfo.Name, colInfo)

	query := fmt.Sprintf("select * from %s", tableInfo.Name)
	Info.Print(query)

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

	for rows.Next() {
		values := make([]interface{}, len(colInfo))
		pointers := make([]interface{}, len(colInfo))
		for i, _ := range values {
			pointers[i] = &values[i]
		}
		err = rows.Scan(pointers...)
		if err != nil {
			Error.Fatal(err)
		}

		query = makeInsertQuery(tableInfo, values, columns, colInfo)
		if containsBrokenChar(query) {

		} else {
			msgQ <- query
		}

	}
}
