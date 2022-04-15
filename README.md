# migrationOracle2Maria
  - 오라클의 데이터를 MariaDB에 등록한다.

## flow
 1. yaml 파일에서 DB 접속 정보를 읽는다.
 2. 오라클 테이블 개수만큼 thread를 생성한다.
 3. 오라클 테이블을 읽어 mariadb에 넣는다.

## 기능
  - 컬럼 매핑은 자동으로하고, 컬럼이름이 다를 경우 yaml 파일에 명시
  - 대상이 아닌 컬럼이 있는 경우 yaml에 명시
  - 배치 commit으로 insert 몇건당 commit 하는지 명시
  - 배치 commit에 실패하면, 개별 commit 으로 재시도
  - insert 하기전에 truncate 할지 yaml에 명시
  - insert 실패하면 별도 로그에 기록
  - euckr 범위 밖의 문자가 포함된 경우 insert 하지 않고 별도 로그에 기록
  
### 순차 테이블
  - tables의 name에 %가 포함되어 있으면 여러 테이블을 의미한다. (순차 테이블로 표현하겠다)
  - '%01d', '%02d', '%03d' 등 formatted 규칙을 지정할 수 있다.
  - 시작 인덱스와 종료 인덱스는 start_idx, end_idx에 지정한다.
  - target_name에 마리아DB의 대상 테이블명을 지정 하여, 오라클의 여러 테이블에서 select 하여, 마리아DB 한개 테이블에 insert 하게된다.
  - thread_count 가 명시되어 있으면, thread_count * index 개수만큼 thread를 만들어서 insert 처리한다.
  ```ymal
tables:
  - name: TEST_TBL_%02d
    start_idx: 0
    end_idx: 10
    target_name: TEST_TBL
    before_truncate: true
  ```
