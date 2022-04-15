package main

import (
	"sync"
)

type CounterInt64 struct {
	count int64
	mu    sync.Mutex
}

type CounterInt struct {
	count int
	mu    sync.Mutex
}

func (c *CounterInt64) Add(n int64) {
	c.mu.Lock()
	c.count += n
	c.mu.Unlock()
}

func (c *CounterInt) Add(n int) {
	c.mu.Lock()
	c.count += n
	c.mu.Unlock()
}

type TableStatus struct {
	oracleRow    int64        // 오라클 전체 record count
	mariaRow     int64        // 마리아 전체 record count
	batchCount   CounterInt64 // batch insert 개수
	retryCount   CounterInt64 // retry insert 개수
	brokenCount  CounterInt64 // 깨진 캐릭터 개수
	dbErrorCount CounterInt64 // DB 에러 개수
	threadCount  CounterInt   // 현재 동작중인 thread count
	wait         sync.WaitGroup
}

func (ts *TableStatus) ToReport() Report {
	var r Report
	r.oracleRow = ts.oracleRow
	r.mariaRow = ts.mariaRow
	r.batchCount = ts.batchCount.count
	r.retryCount = ts.retryCount.count
	r.brokenCount = ts.brokenCount.count
	r.dbErrorCount = ts.dbErrorCount.count

	return r
}
