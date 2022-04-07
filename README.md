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
