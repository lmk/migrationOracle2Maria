# migrationOracle2Maria
  - Oracle의 데이터를 Select 해서 MariaDB에 Insert 한다.

## flow
 1. yaml 파일에서 DB 접속 정보를 읽는다.
 2. Oracle 테이블 목록을 읽는다.
 3. Oracle 테이블의 데이터를 조회해서 mariadb에 넣는다.

## 기능
  - Oracle 접속 정보를 yaml에 명시
  - MariaDB 접속 정보를 yaml에 명시
  - 테이블명이 같은 경우 yaml에 명시 하지 않는다
  - 컬럼 매핑은 자동
  - 대상이 아닌 컬럼이 있는 경우 yaml에 명시
  - 배치 commit으로 insert 몇 건당 commit 하는지 yaml에 명시
  - 배치 commit에 실패하면, 개별 commit 으로 재시도
  - insert 하기전에 truncate 할지 yaml에 명시
  - euckr로 로그를 기록할지 yaml에 명시
  - insert 실패하면 별도 로그에 기록
  - euckr 범위 밖의 문자가 포함된 경우 insert 하지 않고 별도 로그에 기록
  - 마이그레이션 처리전에 수행할 쿼리가 있는 경우 yaml에 명시
  - 마이그레이션 처리후에 수행할 쿼리가 있는 경우 yaml에 명시
  - 파일로 로그를 기록할지 arguement 로 명시
  - 마이그레이션 처리를 순차 처리할지 병렬 처리할지 arguement 로 명시
  - 상세 trace 로그를 출력할지 arguement 로 명시
  - 어떤 yaml 파일을 읽을지 arguement 로 명시
  
### 순차 테이블
  - tables의 name에 %가 포함되어 있으면 여러 테이블을 의미한다. (순차 테이블로 표현하겠다)
  - '%01d', '%02d', '%03d' 등 formatted 규칙을 지정할 수 있다.
  - 시작 인덱스와 종료 인덱스는 start_idx, end_idx에 지정한다.
  - target_name에 마리아DB의 대상 테이블명을 지정 하여, Oracle의 여러 테이블에서 select 하여, 마리아DB 한개 테이블에 insert 하게된다.
  - thread_count 가 명시되어 있으면, thread_count * index 개수만큼 thread를 만들어서 insert 처리한다.
  ```yaml
tables:
  - name: TEST_TBL_%02d
    start_idx: 0
    end_idx: 10
    target_name: TEST_TBL
    before_truncate: true
  ```

### argeuement
```shell
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
