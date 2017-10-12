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


package ring

import "encoding/binary"
import "bytes"
import "github.com/valyala/bytebufferpool"
import "github.com/maxymania/gobase/genericstruct"


var NodeMaster = &genericstruct.NodeMaster{Factory:NodeConstructor}

type NodeHead struct{
	Next, Prev int64
	Tag, Content   int32
}

type Node struct{
	Head        NodeHead
	Tag,Content []byte
	Tainted     bool
}
func NodeConstructor() genericstruct.Block { return new(Node) }
func (n *Node) Load(buf *bytebufferpool.ByteBuffer) {
	src := bytes.NewReader(buf.B)
	binary.Read(src,binary.BigEndian,&(n.Head))
	n.Tag     = make([]byte,int(n.Head.Tag    ))
	n.Content = make([]byte,int(n.Head.Content))
	src.Read(n.Tag    )
	src.Read(n.Content)
	n.Tainted = false
}
func (n *Node) Store(buf *bytebufferpool.ByteBuffer) {
	n.Head.Tag     = int32(len(n.Tag))
	n.Head.Content = int32(len(n.Content))
	binary.Write(buf,binary.BigEndian,n.Head)
	buf.Write(n.Tag)
	buf.Write(n.Content)
	n.Tainted = false
}
func (n *Node) Dirty() bool { return n.Tainted }

type ListManager struct{
	Cache *genericstruct.NodeCache
}

func (l *ListManager) Init(ring int64) error {
	b,e := l.Cache.Get(ring) ; if e!=nil { return e }
	n := b.(*Node)
	n.Head.Prev = ring
	n.Head.Next = ring
	n.Tainted = true
	return nil
}

// A <-> C to A <-> B <-> C
func (l *ListManager) InsertAfter(ring, other int64) error {
	// 1 <-> 3 to 1 <-> 2 <-> 3
	i1,i2 := ring,other
	b1,e := l.Cache.Get(i1) ; if e!=nil { return e }
	n1 := b1.(*Node)
	b2,e := l.Cache.Get(i2) ; if e!=nil { return e }
	n2 := b2.(*Node)
	i3 := n1.Head.Next
	b3,e := l.Cache.Get(i3) ; if e!=nil { return e }
	n3 := b3.(*Node)
	
	// 1 -> 2 -> 3
	n1.Head.Next = i2
	n2.Head.Next = i3
	
	// 1 <- 2 <- 3
	n3.Head.Prev = i2
	n2.Head.Prev = i1
	
	n1.Tainted = true
	n2.Tainted = true
	n3.Tainted = true
	return nil
}
// A <-> C to A <-> B <-> C
func (l *ListManager) InsertBefore(ring, other int64) error {
	// 1 <-> 3 to 1 <-> 2 <-> 3
	i3,i2 := ring,other
	b3,e := l.Cache.Get(i3) ; if e!=nil { return e }
	n3 := b3.(*Node)
	b2,e := l.Cache.Get(i2) ; if e!=nil { return e }
	n2 := b2.(*Node)
	i1 := n3.Head.Prev
	b1,e := l.Cache.Get(i1) ; if e!=nil { return e }
	n1 := b1.(*Node)
	
	// 1 -> 2 -> 3
	n1.Head.Next = i2
	n2.Head.Next = i3
	
	// 1 <- 2 <- 3
	n3.Head.Prev = i2
	n2.Head.Prev = i1
	
	n1.Tainted = true
	n2.Tainted = true
	n3.Tainted = true
	return nil
}

// A <-> B <-> C to A <-> C
func (l *ListManager) Remove(ring int64) error {
	i2 := ring
	
	b2,e := l.Cache.Get(i2) ; if e!=nil { return e }
	n2 := b2.(*Node)
	
	i1,i3 := n2.Head.Prev,n2.Head.Next
	
	b1,e := l.Cache.Get(i1) ; if e!=nil { return e }
	n1 := b1.(*Node)
	b3,e := l.Cache.Get(i3) ; if e!=nil { return e }
	n3 := b3.(*Node)
	
	n1.Head.Next = i3
	n3.Head.Prev = i1
	
	n2.Head.Next = 0
	n2.Head.Prev = 0
	
	n1.Tainted = true
	n2.Tainted = true
	n3.Tainted = true
	return nil
}
func (l *ListManager) Next(it int64) (ref int64,dat *Node,err error) {
	var b genericstruct.Block
	b,err = l.Cache.Get(it)
	if err!=nil { return }
	ref = b.(*Node).Head.Next
	b,err = l.Cache.Get(ref)
	if err!=nil { return }
	dat = b.(*Node)
	return
}
func (l *ListManager) Prev(it int64) (ref int64,dat *Node,err error) {
	var b genericstruct.Block
	b,err = l.Cache.Get(it)
	if err!=nil { return }
	ref = b.(*Node).Head.Prev
	b,err = l.Cache.Get(ref)
	if err!=nil { return }
	dat = b.(*Node)
	return
}
