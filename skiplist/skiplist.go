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


package skiplist

import "github.com/maxymania/gobase/dataman"
import "encoding/binary"
import "github.com/maxymania/gobase/buffer"

const levels = 20


func encode(b []byte,n []int64) {
	for _,num := range n {
		binary.BigEndian.PutUint64(b,uint64(num))
		b = b[8:]
	}
}
func decode(b []byte,n []int64) {
	for i := range n {
		n[i] = int64(binary.BigEndian.Uint64(b))
		b = b[8:]
	}
}

type shared struct{
	dman dataman.DataManager
	cache map[int64]*node
}

type node struct{
	*shared
	offset  int64
	raw     [levels]int64
	levels  [levels]*node
	dirty   [levels]bool
	
	value   int64
	key     []byte
}
func (s *shared) openNode(off int64) (*node,error) {
	if h,ok := s.cache[off] ; ok { return h,nil }
	
	b := buffer.Get((levels*8)+8+4)
	defer buffer.Put(b)
	buf := *b
	_,err := s.dman.RollbackFile().ReadAt(buf[:(levels*8)+8+4],off)
	if err!=nil { return nil,err }
	
	n := new(node)
	n.offset = off
	decode(buf,n.raw[:])
	n.shared = s
	n.value = int64(binary.BigEndian.Uint64(buf[(levels*8):]))
	kl := int32(binary.BigEndian.Uint32(buf[(levels*8)+8:]))
	n.key = make([]byte,kl)
	n.dman.RollbackFile().ReadAt(n.key,off + ((levels*8)+8+4))
	
	s.cache[off] = n
	
	return n,nil
}

func (n *node) loadPointers() {
	
}
func (n *node) persistPointers() error {
	b := buffer.Get((levels*8))
	defer buffer.Put(b)
	for i,l := range n.levels {
		if l!=nil && n.raw[i]==0 {
			n.raw[i] = l.offset
		} else if l==nil { n.raw[i] = 0 }
	}
	encode(*b,n.raw[:])
	E := levels
	B := 0
	for {
		if n.dirty[E-1] { break }
		E--
		if E==0 { break }
	}
	for B<E {
		if n.dirty[B] { break }
		B++
	}
	if B>=E { return nil }
	for i := range n.dirty { n.dirty[i] = false }
	buf := (*b)[:E*8][B*8:]
	_,err := n.dman.RollbackFile().WriteAt(buf,n.offset+int64(B*8))
	return err
}
func (n *node) setNode(i int, m *node) {
	n.dirty[i] = n.levels[i]!=m 
	n.levels[i] = m
}
func (n *node) length() int {
	return (levels*8)+8+4+len(n.key)
}
func (n *node) persist() error {
	lng := n.length()
	if n.offset==0 {
		off,err := n.dman.Alloc(int64(lng))
		if err!=nil { return err }
		n.offset = off
	}
	
	b := buffer.Get(lng-(levels*8))
	defer buffer.Put(b)
	buf := (*b)[:lng-(levels*8)]
	binary.BigEndian.PutUint64(buf,uint64(n.value))
	binary.BigEndian.PutUint64(buf[8:],uint64(len(n.key)))
	copy(buf[8+4:],n.key)
	_,err := n.dman.RollbackFile().WriteAt(buf,n.offset+int64((levels*8)))
	return err
}

