//  package hdb reads history database files persisted in the binary HDB format and extracts the history rows (time/value pairs).
package hdb

import (
	"os"
	"fmt"
	"encoding/binary"
	"io"
	"time"
)

//  Record is a history entry - a time/value pair
type Record struct {
	Value float32
	Time time.Time
}

func endsWith(s string, suffix string) bool {
	if len(suffix) > len(s) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

// Dumb and slow way. Doesn't really matter.
func advanceToNextMultipleOf16(i *int64) {
	for {
		if *i%16==0 {
			return
		}
		*i++
	}
}

func goToPreviousMultipleOf16(i *int64) {
	for {
		if *i%16==0 {
			return
		}
		*i--
	}
}

func scanRow(r io.Reader) (v Record, err error){
	var bogus int32
	if err = binary.Read(r, binary.BigEndian, &bogus); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &v.Value); err != nil {
		return
	}
	// times are ms offset from epoch
	var offset int64
	if err = binary.Read(r, binary.BigEndian, &offset); err != nil {
		return
	}
	v.Time = time.Unix(0, offset*int64(time.Millisecond))
	return
}

//  Read takes the given ReadSeeker and returns all the history records it contains as well as any error
func Read(r io.ReadSeeker) (ret []Record, err error){
	var currentOffset int64

	// Read the first 12 bytes - as far as I can tell they are just magic
	var (
		magic1 uint64
		magic2 uint32
	)
	if err = binary.Read(r, binary.BigEndian, &magic1); err != nil {
		err = fmt.Errorf("error reading magic 1")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &magic2); err != nil {
		err = fmt.Errorf("error reading magic 2")
		return
	}
	currentOffset += 12

	//  Scan the XML - read until we get </bajaObjectGraph>
	var xml []byte
	for {
		var x byte
		if err = binary.Read(r, binary.BigEndian, &x); err != nil {
			err = fmt.Errorf("error scanning XML header")
			return
		}
		xml = append(xml, x)
		currentOffset += 1
		if endsWith(string(xml), "</bajaObjectGraph>"){
			break
		}
	}
	advanceToNextMultipleOf16(&currentOffset)
	r.Seek(currentOffset, 0)  // whence = 0 means relative to start of file

	// Scan rows until we find the first non-zero one
	var (
		i1 int64
		i2 int64
	)
	for {
		if err = binary.Read(r, binary.BigEndian, &i1); err != nil {
			err = fmt.Errorf("could not scan padding row")
			return
		}
		if err = binary.Read(r, binary.BigEndian, &i2); err != nil {
			err = fmt.Errorf("could not scan padding row")
			return
		}
		if i1 != 0 || i2 != 0 {
			break
		}
	}

	goToPreviousMultipleOf16(&currentOffset) // Go back to start of row
	r.Seek(currentOffset, 0)

	for {
		row, errI := scanRow(r)
		if errI != nil {
			errI = err
			return
		}
		ret = append(ret, row)
	}

}

//  ReadFile reads the given file and returns the history records it contains and any error
func ReadFile(path string) ([]Record, error){
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(f)
}