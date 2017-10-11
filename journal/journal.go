/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package journal

import "github.com/cznic/file"
import "fmt"
import "github.com/maxymania/gobase/overlay"
import "github.com/maxymania/gobase/diagnostics"
import "io"
import "os"


func bzero(b []byte) {
	for i := range b { b[i] = 0 }
}

type ECommitError struct{ Inner error }
func (e *ECommitError) Error() string { return fmt.Sprintf("Commit error: %v",e.Inner) }

type WAL_Target interface{
	io.ReadWriteSeeker
	Truncate(int64) error
}

type fileInfo struct{
	os.FileInfo
	size int64
}
func (f *fileInfo) Size() int64 { return f.size }

/*
A file-Wrapper that features Write ahead logging.
*/
type JournalFile struct{
	file.File
	overlay *overlay.Overlay
}
func OpenJournalFile(f file.File,w WAL_Target) (*JournalFile,error) {
	j := new(JournalFile)
	j.File = f
	j.overlay = overlay.NewOverlay()
	
	// Recover Journal if needed.
	p,err := w.Seek(0,0)
	if err!=nil && err!=io.EOF { return nil,err }
	if p>0 {
		err := j.overlay.LoadJournal(w)
		if err!=nil { return nil,err }
		err = j.overlay.ApplyTo(f)
		if err!=nil { return nil,err }
		j.overlay.ClearJournal()
	}
	return j,nil
}

func (j *JournalFile) ReadAt(p []byte, off int64) (n int, err error) {
	cursize := j.overlay.GetCurrentSize()
	max := off-cursize
	if cursize<0 { max = int64(len(p)) }
	if off<0 { return 0,io.EOF }
	isEOF := int64(len(p))>max
	if isEOF { p = p[:int(max)] }
	n,err = j.File.ReadAt(p,off)
	if n<len(p) && int64(n)<max {
		bzero(p[n:])
		n = len(p)
		if err==io.EOF { err = nil }
	}
	j.overlay.ReadOverAt(p,off)
	if isEOF && err==nil { err = io.EOF }
	return
}
func (j *JournalFile) WriteAt(p []byte, off int64) (n int, err error) {
	return j.overlay.WriteAt(p,off)
}
func (j *JournalFile) Truncate(i int64) error {
	return j.overlay.Truncate(i)
}
func (j *JournalFile) Stat() (os.FileInfo, error) {
	n := j.overlay.GetCurrentSize()
	f,e := j.File.Stat()
	if e==nil && n>=0 {
		f = &fileInfo{f,n}
	}
	return f,e
}
func (j *JournalFile) Commit(rws WAL_Target) error {
	rws.Seek(0,0)
	err := j.overlay.DumpJournal(rws) // Dump Changes into Write-Ahead Log.
	if err!=nil { return err }
	rws.Seek(0,0)
	err = j.overlay.ApplyTo(j.File) // Apply Changes
	if err!=nil { return &ECommitError{err} }
	err = rws.Truncate(0) // Delete Write-Ahead Log.
	if err!=nil { return err }
	j.overlay.ClearJournal()
	return nil
}
func (j *JournalFile) String() string {
	return fmt.Sprint(j.overlay)
}

/*
Get the estiminated size of the Write-Ahead log in bytes.
*/
func (j *JournalFile) GetWalSize() int64 {
	w := new(diagnostics.CountWriter)
	j.overlay.DumpJournal(w)
	return int64(*w)
}

