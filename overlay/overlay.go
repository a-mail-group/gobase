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


package overlay

import "github.com/tidwall/btree"
import "fmt"
import "bytes"
import "github.com/vmihailenco/msgpack"
import "io"
import "github.com/maxymania/gobase/buffer"

type Output interface{
	Truncate(int64) error
	WriteAt(p []byte, off int64) (n int, err error)
}

type item struct{
	offset int64
	data   []byte
	alloc  *[]byte
}
func (a *item) Less(than btree.Item, ctx interface{}) bool {
	b := than.(*item)
	return a.offset<b.offset
}
func (i *item) end() int64 { return i.offset + int64(len(i.data)) }
func (i *item) free() {
	buffer.Put(i.alloc)
	i.alloc = nil
}
func (i *item) allocate(data []byte) *item {
	i.alloc = buffer.Get(len(data))
	i.data = (*i.alloc)[:len(data)]
	copy(i.data,data)
	return i
}

type Overlay struct{
	sl       *btree.BTree
	fileSize int64
	truncate bool
}
func NewOverlay() *Overlay {
	return &Overlay{sl:btree.New(2,nil)}
}
func (o *Overlay) cutFileSize(cutlim int64){
	for {
		elem := o.sl.DeleteMax()
		if elem==nil { return }
		ne := elem.(*item)
		if ne.offset>=cutlim { ne.free(); continue }
		if ne.end()>cutlim {
			l := int(cutlim-ne.offset)
			ne.data = ne.data[:l]
		}
		o.sl.ReplaceOrInsert(ne)
		return
	}
}
func (o *Overlay) Truncate(i int64) error {
	if o.truncate {
		if o.fileSize > i { o.fileSize = i }
		o.cutFileSize(i)
		return nil
	}
	o.truncate = true
	o.fileSize = i
	o.cutFileSize(i)
	return nil
}
func (o *Overlay) WriteAt(p []byte, off int64) (n int, err error) {
	var sbuf [128]*item
	dbuf := sbuf[:0]
	n = len(p)
	
	if off<0 { return 0,fmt.Errorf("Invalid offset %v",off) }
	end := off+int64(n)
	search := &item{offset:off}
	
	o.sl.DescendLessOrEqual(search,func(i btree.Item) bool {
		ne := i.(*item)
		if off<ne.end() {
			/*
			 * Buffer Length is ne.end()-ne.offset, and ne.end() is > off,
			 * so ne.end()-ne.offset > off-ne.offset
			 */
			l   := int(off-ne.offset)
			wl  := copy(ne.data[l:],p)
			p    = p[wl:]
			off += int64(wl)
		}
		return false // we are only interested in one.
	})
	search.offset = off
	o.sl.AscendGreaterOrEqual(search,func(i btree.Item) bool {
		ne := i.(*item)
		if end <= ne.offset { return false } // element begins after the region to be written
		if off < ne.offset {
			l    := int(ne.offset-off)
			dbuf  = append(dbuf,(&item{offset:off}).allocate(p[:l]))
			p     = p[l:]
			off  += int64(l)
		}
		// Lemma: ne.offset <= off
		if ne.offset<off { fmt.Println("WARNING: ERROR") ; return false } // BUG! Propably Overlapping elements.
		{
			l    := copy(ne.data,p)
			p     = p[l:]
			off  += int64(l)
		}
		return true
	})
	if off<end {
		l    := int(end-off)
		dbuf  = append(dbuf,(&item{offset:off}).allocate(p[:l]))
		p     = p[l:]
		off  += int64(l)
	}
	for _,ne := range dbuf {
		o.sl.ReplaceOrInsert(ne)
	}
	
	return
}
func (o *Overlay) String() string {
	if o==nil { return "<nil>" }
	if o.sl==nil { return "{nil}" }
	var buf bytes.Buffer
	buf.WriteString("{")
	o.sl.Ascend(func (i btree.Item) bool {
		ne := i.(*item)
		buf.WriteString(fmt.Sprintf("[%d-%d],",ne.offset,ne.end()))
		return true
	})
	if o.truncate {
		buf.WriteString(fmt.Sprintf("truncate %d",o.fileSize))
	}
	buf.WriteString("}")
	return buf.String()
}
func (o *Overlay) ApplyTo(dest Output) error {
	if o.truncate {
		err := dest.Truncate(o.fileSize)
		if err!=nil { return err }
	}
	var e error
	e = nil
	o.sl.Ascend(func (i btree.Item) bool {
		ne := i.(*item)
		_,e = dest.WriteAt(ne.data,ne.offset)
		return e==nil
	})
	return e
}
func (o *Overlay) GetCurrentSize() (n int64) {
	n = -1
	if o.truncate { n = o.fileSize }
	elem := o.sl.Max()
	if elem!=nil {
		end := elem.(*item).end()
		if end>n { n = end }
	}
	return
}
func (o *Overlay) DumpJournal(target io.Writer) (err error) {
	enc := msgpack.NewEncoder(target)
	err = enc.Encode(o.truncate,o.fileSize)
	if err!=nil { return }
	o.sl.Ascend(func (i btree.Item) bool {
		ne := i.(*item)
		err = enc.Encode(true,ne.offset,ne.data)
		return err==nil
	})
	err = enc.Encode(false,int64(0),[]byte{})
	return
}
func (o *Overlay) LoadJournal(source io.Reader) (err error) {
	dec := msgpack.NewDecoder(source)
	err = dec.Decode(&(o.truncate),&(o.fileSize))
	if err!=nil { return }
	var ok bool
	var off int64
	var data []byte
	for {
		err = dec.Decode(&ok,&off,&data)
		if err!=nil { return }
		if !ok { break }
		ne := &item{offset:off,data:data}
		o.sl.ReplaceOrInsert(ne)
	}
	return
}
func (o *Overlay) ReadOverAt(p []byte, off int64) int {
	n := len(p)
	end := int64(n)+off
	o.sl.AscendGreaterOrEqual(&item{offset:off},func(i btree.Item) bool {
		ne := i.(*item)
		if end <= ne.offset { return false } // element begins after the region to be read
		
		// skip gap
		if off < ne.offset {
			l    := int(ne.offset-off)
			off  += int64(l)
			p     = p[l:]
		}
		
		if ne.offset<off { fmt.Println("WARNING: ERROR") ; return false } // BUG! Propably Overlapping elements.
		
		{ // Read part.
			l    := copy(p,ne.data)
			p     = p[l:]
			off  += int64(l)
		}
		return true
	})
	//n -= len(p)
	if o.truncate {
		if off<o.fileSize {
			if end<=o.fileSize { return n }
			return n-int(end-o.fileSize)
		}
	}
	return n-len(p)
}
func (o *Overlay) ClearJournal() {
	o.truncate = false
	for i,n := 0,o.sl.Len() ; i<n ; i++ {
		o.sl.DeleteMax().(*item).free()
	}
}


