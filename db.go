package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

func ConnectMaria() *sql.DB {
	maria, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?autocommit=true", conf.Maria.User, conf.Maria.Password, conf.Maria.Ip, conf.Maria.Port, conf.Maria.Database))
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

// truncateTable mariadb 테이블을 truncate 한다.
func truncateTable(tableName string) {

	maria := ConnectMaria()
	defer maria.Close()

	_, err := maria.Exec(fmt.Sprintf("truncate table %s", tableName))
	if err != nil {
		Error.Fatal(err)
	}
}

func execQuery(db *sql.DB, query string) error {
	result, err := db.Exec(query)

	if err != nil {
		return err
	}

	//n, err := result.RowsAffected()
	// if err != nil {
	// 	return err
	// }

	// if n <= 0 {
	// 	return errors.New("rowsaffected is 0")
	// }

	// mariadb 에서 insert 문은 LastInsertId()에 성공 결과가 담긴다.
	if strings.HasPrefix(strings.ToLower(strings.Trim(query, " \n\r\t")), "insert") {
		l, err := result.LastInsertId()
		if err != nil {
			return err
		}

		if l <= 0 {
			return errors.New("lastinsertid is 0")
		}
	}

	return nil
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
	maria := ConnectMaria()
	defer maria.Close()

	return getDbCount(maria, tableName)
}
