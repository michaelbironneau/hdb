package hdb

import (
	"testing"
	"time"
)

func testFile(path string, t *testing.T){
	rows, err := ReadFile("test.hdb")
	if err != nil {
		t.Errorf("error parsing test.hdb: %v", err)
		return
	}
	if len(rows) == 0 {
		t.Errorf("no rows returned")
		return
	}
	rowMidpoint := int(0.5*float64(len(rows)))
	if rows[rowMidpoint].Time.Before(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)){
		t.Errorf("time incorrectly parsed: %v", rows[rowMidpoint])
		return
	}
	if rows[rowMidpoint].Value > 100 || rows[rowMidpoint].Value < 0 {
		t.Errorf("value incorrectly parsed: %v", rows[rowMidpoint])
		return
	}
}

func TestParsing(t *testing.T){
	testFile("test.hdb", t)
	testFile("test2.hdb", t)
}
