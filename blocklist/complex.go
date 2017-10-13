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


package blocklist

import "github.com/maxymania/gobase/dataman"
import "encoding/binary"
import "io"
import "errors"

type Int64 [8]byte
func (b *Int64) Int64() int64 { return int64(binary.BigEndian.Uint64(b[:])) }
func (b *Int64) SetInt64(i int64) { binary.BigEndian.PutUint64(b[:],uint64(i)) }

type Int32 [4]byte
func (b *Int32) Int32() int32 { return int32(binary.BigEndian.Uint32(b[:])) }
func (b *Int32) SetInt32(i int32) { binary.BigEndian.PutUint32(b[:],uint32(i)) }
func (b *Int32) Int() int { return int(binary.BigEndian.Uint32(b[:])) }
func (b *Int32) SetInt(i int) { binary.BigEndian.PutUint32(b[:],uint32(i)) }


type IntP []byte
func (b IntP) Int64() int64 { return int64(binary.BigEndian.Uint64(b)) }
func (b IntP) SetInt64(i int64) { binary.BigEndian.PutUint64(b,uint64(i)) }
func (b IntP) Int32() int32 { return int32(binary.BigEndian.Uint32(b)) }
func (b IntP) SetInt32(i int32) { binary.BigEndian.PutUint32(b,uint32(i)) }
func (b IntP) Int() int { return int(binary.BigEndian.Uint32(b)) }
func (b IntP) SetInt(i int) { binary.BigEndian.PutUint32(b,uint32(i)) }

var EIllegalPosition = errors.New("Illegal Position (0)")

type BufAddr struct{
	Off int64
	Len int
}

var sizes = [...]int{
	0x80000-16,
	0x40000-16,
	0x20000-16,
	0x10000-16,
}
const (
	o_next = 0
	o_first
	o_cap = 8
	o_last
	o_len = 12
)
// [ First:8 | Last:8 ]

// [ Next:8 | Cap:4 | Len:4 ]

// Allocate and Clear List-Head
func NewListHead(DM dataman.DataManager) (int64,error) {
	off,err := DM.Alloc(16)
	if err!=nil { return 0,err }
	
	_,err = DM.RollbackFile().WriteAt(make([]byte,16),off)
	
	if err!=nil { return 0,err }
	
	return off,nil
}


func Allocate(DM dataman.DataManager,n int) (baa []BufAddr,err error) {
	var off,lng int64
	for _,size := range sizes {
		for size<n {
			off,err = DM.Alloc(int64(size)+16)
			if err!=nil { return }
			lng,err = DM.UsableSize(off)
			if err!=nil { return }
			baa = append(baa,BufAddr{off,int(lng-16)})
			n-=int(lng-16)
		}
	}
	{
		off,err = DM.Alloc(int64(n)+16)
		if err!=nil { return }
		lng,err = DM.UsableSize(off)
		if err!=nil { return }
		baa = append(baa,BufAddr{off,int(lng)})
	}
	return
}

func Chainify(DM dataman.DataManager,baa []BufAddr,off int64) (err error) {
	if off==0 { return EIllegalPosition }
	var head,elem [16]byte
	fl := DM.RollbackFile()
	
	_,err = fl.ReadAt(head[:],off)
	if err!=nil { return }
	
	hasNoTail := IntP(head[8:]).Int64()==0
	
	copy(elem[:8],head[:8])
	IntP(elem[12:]).SetInt32(0)
	
	i := len(baa)
	for 0<i {
		i--
		IntP(elem[8:]).SetInt(baa[i].Len)
		_,err = fl.WriteAt(elem[:],baa[i].Off)
		if err!=nil { return }
		IntP(elem[:]).SetInt64(baa[i].Off)
	}
	
	if hasNoTail {
		IntP(head[8:]).SetInt64(baa[len(baa)-1].Off)
	}
	copy(head[:8],elem[:8])
	_,err = fl.WriteAt(head[:],off)
	
	return
}

// Adds off2 to off1. off2 should be cleared, as it is not done automatically.
func AddElementsAndFree(DM dataman.DataManager, off1, off2 int64) (err error) {
	if off1==0 { return EIllegalPosition }
	if off2==0 { return EIllegalPosition }
	var head1,head2 [16]byte
	fl := DM.RollbackFile()
	
	_,err = fl.ReadAt(head1[:],off1) ; if err!=nil { return }
	
	_,err = fl.ReadAt(head2[:],off2) ; if err!=nil { return }
	
	if IntP(head2[8:]).Int64()==0 { return }
	if IntP(head1[8:]).Int64()==0 { // Copy off2 to off1.
		_,err = fl.ReadAt(head2[:],off1)
		return
	}
	
	// off1->Tail->Next = off2->Head
	_,err = fl.WriteAt(head2[:8],IntP(head1[8:]).Int64()) ; if err!=nil { return }
	
	// off1->Tail = off2->Tail
	copy(head1[8:],head2[8:])
	_,err = fl.WriteAt(head1[8:],8+off1) //; if err!=nil { return }
	
	return
}

func IterateOverList(r io.ReaderAt, head int64, max int) (nhd int64,i []int64,err error) {
	if head==0 { return 0,nil,EIllegalPosition }
	var i64 Int64
	
	// Get head->Head
	_,err = r.ReadAt(i64[:],head) ; if err!=nil { return }
	nhd = i64.Int64()
	
	i = make([]int64,0,max)
	for ; max>0 ; max--{
		if nhd==0 { break }
		i = append(i,nhd)
		
		// nhd = nhd->Next
		_,err = r.ReadAt(i64[:],nhd) ; if err!=nil { return }
		nhd = i64.Int64()
	}
	
	return
}
func IterateOverListEx(r io.ReaderAt, head int64, max int) (nhd int64,i []BufAddr,err error) {
	if head==0 { return 0,nil,EIllegalPosition }
	var i32 Int32
	var i64 Int64
	
	// Get head->Head
	_,err = r.ReadAt(i64[:],head) ; if err!=nil { return }
	nhd = i64.Int64()
	
	i = make([]BufAddr,0,max)
	for ; max>0 ; max--{
		if nhd==0 { break }
		
		_,err = r.ReadAt(i64[:],nhd) ; if err!=nil { return }
		_,err = r.ReadAt(i32[:],nhd+o_cap) ; if err!=nil { return }
		
		i = append(i,BufAddr{nhd,i32.Int()})
		
		// nhd = nhd->Next
		nhd = i64.Int64()
	}
	
	return
}


func SetHead(w io.WriterAt, off int64, newHead int64) (err error) {
	if off==0 { return EIllegalPosition }
	var buf [16]byte
	for i := range buf { buf[i] = 0 }
	
	if newHead==0 {
		// off->Head = 0 ; off->Tail = 0
		_,err = w.WriteAt(buf[:],off)
	} else {
		// off->Head = newHead
		IntP(buf[:]).SetInt64(newHead)
		_,err = w.WriteAt(buf[:8],off)
	}
	return
}

// Sets the length field where the LSB is used for the lastInChain boolean
func SetExtendedLen(w io.WriterAt, off int64, lng int, lastInChain bool) error {
	if off==0 { return EIllegalPosition }
	var i32 Int32
	lng<<=1
	if lastInChain { lng |=1 }
	i32.SetInt(lng)
	_,err := w.WriteAt(i32[:],off+o_len)
	return err
}

func GetExtendedLen(r io.ReaderAt, off int64) (lng int, lastInChain bool, err error) {
	if off==0 { return 0,false,EIllegalPosition }
	var i32 Int32
	
	_,err = r.ReadAt(i32[:],off+o_len)
	lng = i32.Int()
	lastInChain = (lng&1)==1
	lng>>=1
	return
}

func GetNext(r io.ReaderAt, off int64) (nxt int64,err error) {
	if off==0 { return 0,EIllegalPosition }
	var i64 Int64
	
	_,err = r.ReadAt(i64[:],off)
	nxt = i64.Int64()
	
	return
}


