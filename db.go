package main

import (
	"database/sql"
	"errors"
	"fmt"
)

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
}

func execQuery(db *sql.DB, query string) error {
	result, err := db.Exec(query)

	if err != nil {
		return err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if n <= 0 {
		return errors.New("rowsaffected is 0")
	}

	l, err := result.LastInsertId()
	if err != nil {
		return err
	}

	if l <= 0 {
		return errors.New("lastinsertid is 0")
	}

	return nil
}
