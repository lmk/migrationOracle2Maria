package main

import (
	"database/sql"
	"fmt"
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
	}

	return colInfoList, nil
}

func ConnectMaria(autoCommit bool) *sql.DB {
	connStr := ""
	if autoCommit {
		connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?autocommit=true", conf.Maria.User, conf.Maria.Password, conf.Maria.Ip, conf.Maria.Port, conf.Maria.Database)
	} else {
		connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.Maria.User, conf.Maria.Password, conf.Maria.Ip, conf.Maria.Port, conf.Maria.Database)
	}

	maria, err := sql.Open("mysql", connStr)
	if err != nil {
		Error.Fatal(err)
	}

	return maria
}

func ConnectOracle() *sql.DB {
	ora, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", conf.Oracle.User, conf.Oracle.Password, conf.Oracle.Database))
	if err != nil {
		Error.Fatal(err)
	}

	return ora
}

// truncateTable 새로 maira connection을 맺고, mariadb 테이블을 truncate 한다.
// 실패하면, Error 출력후 종료한다.
func truncateTable(tableName string) {

	maria := ConnectMaria(true)
	defer maria.Close()

	_, err := maria.Exec(fmt.Sprintf("truncate table %s", tableName))
	if err != nil {
		Error.Fatal(err)
	}
}

func execQuery(db *sql.DB, query string) error {
	_, err := db.Exec(query)

	if err != nil {
		return err
	}

	return nil
}

// execNewQuery 새로 maira connection을 맺고, 쿼리를 날린다.
// 실패하면, Error 출력후 종료한다.
func execNewQuery(query string) {

	maria := ConnectMaria(true)
	defer maria.Close()

	err := execQuery(maria, query)
	if err != nil {
		Error.Fatal(err.Error() + "," + query)
	}
}

func getDbCount(conn *sql.DB, tableName string) int64 {
	var count int64
	err := conn.QueryRow("select count(*) from " + tableName).Scan(&count)
	if err != nil {
		Error.Panic(err)
	}

	return count
}

func getOracleCount(tableName string) int64 {
	ora := ConnectOracle()
	defer ora.Close()

	return getDbCount(ora, tableName)
}

func getMairaCount(tableName string) int64 {
	maria := ConnectMaria(true)
	defer maria.Close()

	return getDbCount(maria, tableName)
}

func startTransaction(conn *sql.DB) *sql.Tx {
	tx, err := conn.Begin()
	if err != nil {
		Error.Fatal(conn)
	}

	return tx
}
