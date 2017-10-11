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


package dataman

import "github.com/cznic/file"

type DataManager interface{
	Close() error
	DirectFile() file.File
	RollbackFile() file.File
	
	Alloc(size int64) (int64, error)
	Free(off int64) error
	UsableSize(off int64) (int64, error)
	Commit() error
}

type SimpleDataManager struct{
	f file.File
	a *file.Allocator
}
func NewSimpleDataManager(f file.File) (*SimpleDataManager,error) {
	a,e := file.NewAllocator(f)
	if e!=nil { return nil,e }
	return &SimpleDataManager{f,a},nil
}

func (s *SimpleDataManager) Close() error { return s.a.Close() }
func (s *SimpleDataManager) DirectFile() file.File { return s.f }
func (s *SimpleDataManager) RollbackFile() file.File { return s.f }

func (s *SimpleDataManager) Alloc(size int64) (int64, error) {
	return s.a.Alloc(size)
}
func (s *SimpleDataManager) Free(off int64) error {
	return s.a.Free(off)
}
func (s *SimpleDataManager) UsableSize(off int64) (int64, error) {
	return s.a.UsableSize(off)
}
func (s *SimpleDataManager) Commit() error { return nil }

