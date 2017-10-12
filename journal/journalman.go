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

/*
Close() error
DirectFile() file.File
RollbackFile() file.File

Alloc(size int64) (int64, error)
Free(off int64) error
UsableSize(off int64) (int64, error)
Commit() error
*/

type JournalDataManager struct{
	wal    WAL_Target
	jfile  *JournalFile
	dfile  file.File
	alloc  *file.Allocator
}
func NewJournalDataManager(f file.File, w WAL_Target) (*JournalDataManager,error) {
	j,err := OpenJournalFile(f,w)
	if err!=nil { return nil,err }
	a,err := file.NewAllocator(j)
	if err!=nil { return nil,err }
	err = j.Commit(w)
	if err!=nil { return nil,err }
	return &JournalDataManager{w,j,f,a},nil
}
func (j *JournalDataManager) Close() error { return j.dfile.Close() }
func (j *JournalDataManager) DirectFile() file.File { return j.dfile }
func (j *JournalDataManager) RollbackFile() file.File { return j.jfile }

func (j *JournalDataManager) Alloc(size int64) (int64, error) { return j.alloc.Alloc(size) }
func (j *JournalDataManager) Free(off int64) error { return j.alloc.Free(off) }
func (j *JournalDataManager) UsableSize(off int64) (int64, error) { return j.alloc.UsableSize(off) }
func (j *JournalDataManager) Commit() error { return j.jfile.Commit(j.wal) }
func (j *JournalDataManager) GetWalSize() int64 { return j.jfile.GetWalSize() }
