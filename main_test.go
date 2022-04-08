package main

import (
	"testing"
)

func TestContainsNoEucKr(t *testing.T) {

	noEucKr := []string{
		"議곗.kr",
		"二쇱뜯㎮㎮me",
		"insert into SFS_SPAM_STRING (`SAVE_DT`,`STRING`,`RMK`,`USE_CNT`,`USED_DT`) values ('20210520155117','議곗.kr','TRAP/MMSC/醫⑸え곕',1,'20210522')",
		"7��dmg*com",
		"TRAP/MMSC/醫⑸え곕",
	}

	eucKr := []string{
		"안녕하세요",
		"abcdefg",
		"12345",
		"awⓟ-ⓙK.com",
	}

	for _, item := range noEucKr {
		if !ContainsNoEucKr(item) {
			t.Error("contains no euc-kr")
		}
	}

	for _, item := range eucKr {
		if ContainsNoEucKr(item) {
			t.Error("contains no euc-kr")
		}
	}

}
