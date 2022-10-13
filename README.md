# migrationOracle2Maria
  - Oracle에서 MariaDB로 DB 전환 프로젝트를 진행하면서 데이터를 마이그레이션해야 했습니다. 별도 솔루션을 사용하기 어려웠고 ssh 터미널에서만 작업이 가능했으며, 마이그레이션 해야하는 테이블이 많은 상황이었습니다. 직접 구현하기로 결정했고, 생산성 좋고 동시성 개발이 편한 go 언어를 사용했습니다. 
  - DB 설정을 euckr로 해야 했습니다. Oracle은 euckr이 아닌 데이터도 저장되는데 MariaDB는 euckr이 아닌 데이터를 저장하려면 오류가 발생하기 때문에, euckr 캐릭터셋 체크 및 로그 저장 기능이 들어갔습니다.
  - 전체 소스코드는 [여기(https://github.com/lmk/migrationOracle2Maria)](https://github.com/lmk/migrationOracle2Maria)서 볼수 있습니다.

## 버그
  - 테이블 하나에 데이터가 많은 경우 처리하지 못하는 오류가 있습니다. 5,600만건이 저장된 테이블이 마이그레이션되지 않았습니다.  

## flow
 1. yaml 파일에서 DB 접속 정보를 읽습니다.
 2. Oracle 테이블 목록을 읽습니다.
 3. Oracle 테이블의 데이터를 조회해서 mariadb에 넣습니다.
 4. 결과를 리포팅합니다.

## 기능
  - Oracle 접속 정보를 yaml에 명시합니다. (`oracle`)
  - MariaDB 접속 정보를 yaml에 명시합니다. (`maria`)
  - 마이그레이션 해야할 테이블 정보를 yaml에 명시합니다. (`tables`)
  - 컬럼 매핑은 자동합니다.
  - 마이그레이션 대상이 아닌 컬럼이 있는 경우 yaml에 명시합니다. (`skip_columns`)
  - 배치 commit으로 insert 몇 건당 commit 하는지 yaml에 명시합니다. (`fetch_size`)
  - 테이블별 insert시 thread 개수를 yaml에 명시합니다. (`thread_count`) 
  - 배치 commit에 실패하면, 개별 commit 으로 재시도합니다.
  - insert 하기전에 truncate 할지 yaml에 명시합니다. (`before_truncate`)
  - insert 실패하면 별도 로그에 기록합니다. (`dberr_log`)
  - euckr로 로그를 기록할지 yaml에 명시합니다. (`euckr_log`)
  - euckr 범위 밖의 문자가 포함된 경우 insert 하지 않고 별도 로그에 기록합니다. (`check_euckr`)
  - 마이그레이션 처리전에 수행할 쿼리가 있는 경우 yaml에 명시합니다. (Ex, 트리거 disable에 설정) (`before_query`)
  - 마이그레이션 처리후에 수행할 쿼리가 있는 경우 yaml에 명시합니다. (Ex, 트리거 enable에 설정) (`after_query`)
  - 파일로 로그를 기록할지 arguement 로 명시합니다. (`-nolog`)
  - 마이그레이션 처리를 순차 처리할지 병렬 처리할지 arguement 로 명시합니다. (`-sequential`)
  - 상세 trace 로그를 출력할지 arguement 로 명시합니다. (`-trace`)
  - 어떤 yaml 파일을 읽을지 arguement 로 명시합니다. (`-config`)

### 순차 테이블
  - yaml 파일 `tables`의 `name`에 %가 포함되어 있으면 여러 테이블을 의미합니다. (순차 테이블로 표현하겠습니다)
  - '%01d', '%02d', '%03d' 등 formatted 규칙을 지정할 수 있습니다.
  - 시작 인덱스와 종료 인덱스는 yaml에 `start_idx`, `end_idx`에 지정한다.
  - target_name에 마리아DB의 대상 테이블명을 지정 하여, Oracle의 여러 테이블에서 select 하여, 마리아DB 한개 테이블에 insert 하게된다.
  - thread_count 가 명시되어 있으면, thread_count * index 개수만큼 thread를 만들어서 insert 처리한다.

  - Ex, TEST_TBL_00 ~ TEST_TBL_10의 데이터를 TEST_TBL로 마이그레이션합니다.

  ```yaml
  tables:
    - name: TEST_TBL_%02d
      start_idx: 0
      end_idx: 10
      target_name: TEST_TBL
      before_truncate: true
  ```

### Usage

```bash
$ ./migrationOracle2Maria --help
Usage: ./migrationOracle2Maria -config="yaml file" [OPTIONS] 
  -config string
        config yaml file name(.yml) (default "config.yml")
  -nolog
        if true, NO file log is written. only stdio
  -sequential
        if true, enable sequential processing by table
  -trace
        if true, enable trace log
```

### yaml example

```yaml
broken_log: "broken.sql"
dberr_log: "dberror.sql"
euckr_log: false
check_euckr: true
oracle:
  database: oradb
  user: user
  password: userpassword
maria:
  ip: 127.0.0.1
  port: 3306
  database: mariadb
  user: user
  password: userpassword
  fetch_size: 5000
  before_truncate: true
  before_query:
    - "update trigger_cfg set trigger_enable=0 where trigger_enable=1"
  after_query:
    - "update trigger_cfg set trigger_enable=1 where trigger_enable=0"
tables:
  - name: HUGE_TABLE
    fetch_size: 10000
    before_truncate: true
    thread_count: 10
  - name: TEST_TABLE_A
  - name: TEST_TABLE_B
  - name: TEST_TABLE_C
  - name: TEST_TABLE_D
  - name: TEST_TABLE_E
  - name: TEST_TABLE_F
    fetch_size: 10000
    thread_count: 10
   - name: TEST_TBL_%02d
     start_idx: 0
     end_idx: 5
     target_name: TEST_TBL
     before_truncate: true
  - name: TEST_TABLE_G
   fetch_size: 10000
   thread_count: 10
```

### report 

- 테이블별 결과가 출력되고, 전체 마이그레이션이 종료되면 몇 개 테이블이 얼마나 걸렸는지 출력됩니다.

```log
INFO: 2022/04/19 17:23:38 report.go:33: TEST_TABLE_A, Report Oracle:418425, Maria:418425, broken:0, dbError:0, batch:418425, retry:0, duration:2m3.649694685s
INFO: 2022/04/19 17:24:54 report.go:33: TEST_TABLE_B, Report Oracle:60923, Maria:60923, broken:0, dbError:0, batch:60923, retry:0, duration:3m20.330187127s
INFO: 2022/04/19 17:26:39 report.go:33: TEST_TABLE_C, Report Oracle:1766214, Maria:1763849, broken:2365, dbError:0, batch:1763849, retry:0, duration:5m4.435543488s
INFO: 2022/04/19 17:26:39 main.go:127: 50 Tables Duration 5m5.202180143s
INFO: 2022/04/19 17:26:39 main.go:129: End Job.
```

- 테이블별 Report 로그에 `Oracle`은 마이그레이션 종료 후 오라클에서 Select count이고, `Maria`는 MariaDB에서 Select count 한 결과이므로 숫자가 같아야 정상입니다.

```log
INFO: 2022/04/19 17:23:38 report.go:33: TM_SFS_SPAM_DATA, Report Oracle:418425, Maria:418425, broken:0, dbError:0, batch:418425, retry:0, duration:2m3.649694685s
```

- MariaDB에 Insert시 DB 에러가 발생하면 dbError 만큼 Oracle 수와 Maria 수에 차이가 나고 별도 sql 파일에 저장됩니다.

```log
ERROR: 2022/04/19 17:21:49 migration.go:112: Error 1062: Duplicate entry '142234' for key 'PRIMARY': insert into TEST_TABLE_A (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('142234','mykey','myvalue','20141124203000')
INFO: 2022/04/19 17:21:59 report.go:33: TEST_TABLE_D, Report Oracle:336, Maria:335, broken:0, dbError:1, batch:303, retry:32, duration:25.452654218s
```

```bash
$ cat dberror.sql
insert into TEST_TABLE_A (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('142234','mykey','myvalue','20141124203000');
commit;
```

- euckr 범위 밖의 문자가 포함되어 있으면 MariaDB에 insert가 실패하므로 broken에 건수가 기록되고, Oracle 수와 Maria수에 차이가 나며, 별도 sql 파일에 저장됩니다.

```log
INFO: 2022/04/19 17:26:39 report.go:33: TEST_TABLE_E, Report Oracle:1766214, Maria:1763849, broken:2365, dbError:0, batch:1763849, retry:0, duration:5m4.435543488s
```

```bash
$ wc -l broken.sql
2366 broken.sql
$ tail broken.sql
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172641','key172641','셀幄寧??3105드','20210524153719');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172642','key172642','피錡糖??','20210609153822');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172643','key172643','피扇樗絹?痼都','20210609153822');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172644','key172644','퓸享윱求溟??jf','20210401163408');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172645','key172645','??씀炷뻤궈','20211103160612');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172646','key172646','致??픽態腔졔','20210524153758');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172647','key172647','낮括恍??','20210617144229');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172648','key172648','싫으??','20210405152955');
insert into TEST_TABLE_E (`SEQ`,`KEY`,`VALUE`,`LASTDATE`) values ('172649','key172649','태榻炳廢２윱??','20210405152955');
commit;
```

### code review

#### 구조

- select를 하는 go 루틴을 하나 만들고, Oracle에서 select 하고, MariaDB insert 문을 만들어서 채널로 보냅니다.

```go
	go newSelect(insertQ, tableInfo, &status)
```

- insert하는 go 루틴을 `thread_count`만큼 만들고, 채널에서 꺼네 MariaDB에 insert하고 `fetch_size`만큼 commit 합니다. insert 과정에서 오류가 발생하면 retry 채널로 보냅니다.

```go
	// thread 개수만큼 maria insert thread 생성
	threadCount := tableInfo.ThreadCount

	for threadCount > 0 {
		status.wait.Add(1)
		go newInsert(threadCount, insertQ, retryQ, tableInfo, &status)
		threadCount--
	}
```

- retry go 루틴을 하나 만들고, 채널에서 꺼네 MariaDB에 한 건씩 insert 합니다.

```go
	go RetryInsert(retryQ, tableInfo.TargetName, &status)
```

> commit이 오래 걸리기 때문에 insert를 모아서 트랜젝션 처리하고 go 루틴으로 병렬 처리합니다. retry는 안정적인 처리를 위해, 단일 스레드에서 insert 하나씩 commit 합니다.   

##### `newSelect`

- Oracle 에서 컬럼명, 자료형, 길이를 조회 가져옵니다.

```go
	query := fmt.Sprintf("select column_name, data_type, data_length from dba_tab_columns where table_name='%s' order by column_id", tableName)
```

- 데이터 조회를 위해 Select 쿼리를 만듭니다. Oracle 자료형의 `DATE`, `RAW`는 MariaDB와 호환되지 않으므로 데이터를 가공해서 조회합니다.

```go
		if col.dataType == "DATE" {
			fields = append(fields, fmt.Sprintf("TO_CHAR(%s, 'YYYYMMDDhh24miss') %s", key, key))
		} else if col.dataType == "RAW" {
			fields = append(fields, fmt.Sprintf("RAWTOHEX(%s) %s", key, key))
		} else {
			fields = append(fields, key)
		}
```

- Oracle 에서 데이터를 조회하고, MariaDB용 Insert 문을 만듭니다. Oracle의 `NULL`, `DATE`, `RAW`는 MariaDB에 맞는 자료형으로 변환하고, text 데이터는 MariaDB에 맞게 가공합니다.

```go
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
```

##### `newInsert`

- insert 채널을 읽어서 실행하고, FetchSize만큼씩 commit 합니다.

```go
	// 트랜잭션 시작
	tx := startTransaction(maria)
  // .....
  // msgQ를 읽어서
	for msg := range insertQ {
    // .....
		_, err := tx.Exec(msg)
    // .....
		// FetchSize 만큼
		if len(buf) >= tableInfo.FetchSize {
			//commit
			err = tx.Commit()
```

##### `RetryInsert`

- retry 채널은 다수의 insert 채널에서 보낼 수도 있고, 보내지 않을 수도 있어서 RetryInsert go 루틴의 종료 시점을 특정하기가 어렵습니다. 그래서, go 루틴 종료 시점은 10초 동안 채널이 비어있고 현재 실행되고 있는 insert thread가 없으면 RetryInsert go 루틴을 종료합니다.

```go
RETRY:
	for {
		select {
		case msg := <-retryQ:
    // .....
		case <-time.After(time.Second * 10): // 10초동안 q가 비어 있으면

			// insertThread가 있는지 체크해서 없으면 종료
			if tableState.threadCount.count <= 0 {
				break RETRY
			}
		}
```

##### Logging

- Trace, Info, Warning, Error 을 선언합니다.

```go
var (
	Trace     *log.Logger
	Info      *log.Logger
	Warning   *log.Logger
	Error     *log.Logger
)
```

- 프로그램이 시작되면 Logger를 `os.Stdout`, `os.Stderr`로 초기화합니다.

```go
	// setting log
	sysLog := InitLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
```

- `InitLogger` 에서 log 출력 양식을 정의합니다.

```go
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
```

- 사용할때는 로그 레벨에 맞춰 사용하면 됩니다.

```go
	Trace.Printf("%s, start retry thread", tableName)
  Info.Printf("%s, truncate table", tableInfo.TargetName)
  Warning.Printf("%s, %3d, fail tx.commit %s", tableInfo.TargetName, threadIndex, err)
  Error.Fatalf("%s, %3d, fail tx.rollback %s", tableInfo.TargetName, threadIndex, err)
```

##### 버전 관리

- 버전은 대부분 하드 코딩으로 소스에 넣는데, 소스 코드는 수정하고 버전 변경을 놓치는 경우가 종종 있습니다. 이를 방지하고자 'x.x.x'는 하드코딩하고 git commit 해시와 빌드 일시를 버전에 포함했습니다.

> 버전 표기법은 [유의적 버전](https://semver.org/lang/ko/)을 참고

- 아래와 같이 버전 정보를 조회할 수있습니다. 

```bash
$ ./migrationOracle2Maria -v
Version: 1.0.0 22.cb480b5
Build Date: 2022-10-13 15:05:33
```

- 빌드는 쉘 스크립트를 실행해서 합니다. 빌드가 완료되면 빌드된 버전이 출력됩니다.

```bash
$ ./make.sh
Version: 1.0.0 22.cb480b5
Build Date: 2022-10-13 15:05:33
```

- 'x.x.x'는 make.sh에 하드 코딩 합니다.

```bash
#!/bin/bash
VERSION=1.0.0
```

- 버전 정보는 make.sh에서 go build 파라미터로 넘깁니다.
```bash
go build -ldflags "-X 'main.VERSION=${VERSION}' -X 'main.BUILDDT=${BUILDDT}'" -o ${TARGET} .
```

- `version.go` 에서 버전 출력을 합니다.

```go
var BUILDDT string
var VERSION string

func showVersion() {

   // .........
			fmt.Printf("Version: %s\n", VERSION)
			fmt.Printf("Build Date: %s\n", BUILDDT)
   // .........
}
```
