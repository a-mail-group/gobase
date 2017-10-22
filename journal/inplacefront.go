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

import "io"
import "encoding/binary"
import "errors"

var ENOSPACE = errors.New("ENOSPACE")

const ipwalOff = 8

type ReaderAtWriterAt interface{
	io.ReaderAt
	io.WriterAt
}
type InplaceWAL_File struct{
	buf [8]byte
	w   ReaderAtWriterAt
	max int64   // Write-Limit
	pos int64   // Position
	limit int64 // Read-Limit
	holdSize bool
}
func (i *InplaceWAL_File) SetHoldSize(on bool) error {
	i.holdSize = on
	return i.persistLimit()
}
func (i *InplaceWAL_File) persistLimit() error {
	if i.holdSize { return nil }
	binary.BigEndian.PutUint64(i.buf[:],uint64(i.limit))
	_,err := i.w.WriteAt(i.buf[:],0)
	return err
}
func (i *InplaceWAL_File) Write(b []byte) (n int,e error) {
	var ove error
	ove = nil
	end := int64(len(b))+i.pos
	if i.max<end {
		if i.max<=i.pos { return 0,ENOSPACE }
		ove = ENOSPACE
		b = b[:int(i.max-i.pos)]
	}
	n,e = i.w.WriteAt(b,ipwalOff+i.pos)
	i.pos += int64(n)
	if i.limit<i.pos {
		i.limit = i.pos
		ne := i.persistLimit()
		if e==nil { e = ne }
	}
	if e==nil { e = ove }
	return
}
func (i *InplaceWAL_File) Read(b []byte) (n int,e error) {
	var ove error
	ove = nil
	end := int64(len(b))+i.pos
	if i.limit<end {
		if i.limit<=i.pos { return 0,io.EOF }
		ove = io.EOF
		b = b[:int(i.limit-i.pos)]
	}
	n,e = i.w.ReadAt(b,ipwalOff+i.pos)
	i.pos += int64(n)
	if e==nil { e = ove }
	return
}
func (i *InplaceWAL_File) Truncate(size int64) error {
	if size>i.max { return ENOSPACE }
	if size<0 { size = 0 }
	i.limit = size
	return i.persistLimit()
}
func (i *InplaceWAL_File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0: i.pos = offset
	case 1: i.pos += offset
	case 2: i.pos = i.limit - offset
	}
	if i.pos<0 { i.pos = 0 }
	if i.pos>i.limit { i.pos = i.limit }
	return i.pos,nil
}
func NewInplaceWAL_File (w ReaderAtWriterAt,max int64) *InplaceWAL_File {
	ip := new(InplaceWAL_File)
	ip.w   = w
	ip.max = max-8
	n,_ := w.ReadAt(ip.buf[:],0)
	if n==8 {
		ip.limit = int64(binary.BigEndian.Uint64(ip.buf[:]))
	}
	return ip
}


