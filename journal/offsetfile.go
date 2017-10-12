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
import "os"

type offsetInfo struct{
	os.FileInfo
	offset int64
}
func (o *offsetInfo) Size() int64 {
	size := o.FileInfo.Size()-o.offset
	if size<0 { size = 0 }
	return size
}

type OffsetFile struct{
	file.File
	Offset int64
}
func (o *OffsetFile) ReadAt(p []byte, off int64) (n int, err error) {
	return o.File.ReadAt(p,off+o.Offset)
}
func (o *OffsetFile) Stat() (os.FileInfo, error) {
	s,e := o.File.Stat()
	if s!=nil { s = &offsetInfo{s,o.Offset} }
	return s,e
}
func (o *OffsetFile) Truncate(i int64) error {
	return o.File.Truncate(i+o.Offset)
}
func (o *OffsetFile) WriteAt(p []byte, off int64) (n int, err error) {
	return o.File.WriteAt(p,off+o.Offset)
}

