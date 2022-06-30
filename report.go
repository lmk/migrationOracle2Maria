package main

import "time"

type Report struct {
	oracleRow    int64 // 오라클 전체 record count
	mariaRow     int64 // 마리아 전체 record count
	batchCount   int64 // batch insert 개수
	retryCount   int64 // retry insert 개수
	brokenCount  int64 // 깨진 캐릭터 개수
	dbErrorCount int64 // DB 에러 개수
}

func (r *Report) Add(left *Report) {
	r.oracleRow += left.oracleRow
	r.mariaRow += left.mariaRow
	r.batchCount += left.batchCount
	r.retryCount += left.retryCount
	r.brokenCount += left.brokenCount
	r.dbErrorCount += left.dbErrorCount
}

func (r *Report) Sum(list []Report) {

	for i := range list {
		r.Add(&list[i])
	}
}

func printReport(tableName string, report *Report, duration time.Duration) {

	// 로그 출력
	Info.Printf("%s, Report Oracle:%d, Maria:%d, broken:%d, dbError:%d, batch:%d, retry:%d, duration:%v",
		tableName,
		report.oracleRow,
		report.mariaRow,
		report.brokenCount,
		report.dbErrorCount,
		report.batchCount,
		report.retryCount,
		duration)

	if report.oracleRow != (report.mariaRow + report.brokenCount + report.dbErrorCount) {
		Error.Printf("%s, Report miss count oracle", tableName)
	}

	if report.mariaRow != (report.batchCount + report.retryCount) {
		Error.Printf("%s, Report miss count maria", tableName)
	}
}

func reportTable(tableInfo Table, report *Report, duration time.Duration) {

	report.oracleRow = getOracleCount(tableInfo.SourceName, tableInfo.Where)

	report.mariaRow = getMairaCount(tableInfo.SourceName)

	printReport(tableInfo.TargetName, report, duration)
}

func reportMultiTables(tableInfo []Table, reports []Report, duration time.Duration) {

	var report Report

	report.Sum(reports)

	ora := ConnectOracle()
	defer ora.Close()

	var sumOracle int64
	for _, info := range tableInfo {
		sumOracle += getDbCount(ora, info.SourceName, "")
	}

	report.oracleRow = sumOracle
	report.mariaRow = getMairaCount(tableInfo[0].TargetName)

	printReport(tableInfo[0].TargetName, &report, duration)
}
