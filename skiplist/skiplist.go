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

import "encoding/binary"
import "bytes"
import "github.com/valyala/bytebufferpool"
import "github.com/maxymania/gobase/genericstruct"

const Steps = 20

var NodeMaster = &genericstruct.NodeMaster{Factory:NodeConstructor}

type NodeHead struct{
	Nexts   [Steps]int64
	Content int64
	Rest    int32
}

type Node struct{
	Head    NodeHead
	Key     []byte
	Tainted bool
}
func NodeConstructor() genericstruct.Block { return new(Node) }
func (n *Node) Load(buf *bytebufferpool.ByteBuffer) {
	src := bytes.NewReader(buf.B)
	binary.Read(src,binary.BigEndian,&(n.Head))
	n.Key = make([]byte,int(n.Head.Rest))
	src.Read(n.Key)
	n.Tainted = false
}
func (n *Node) Store(buf *bytebufferpool.ByteBuffer) {
	n.Head.Rest = int32(len(n.Key))
	binary.Write(buf,binary.BigEndian,n.Head)
	buf.Write(n.Key)
	n.Tainted = false
}
func (n *Node) Dirty() bool { return n.Tainted }

type KeySearcher struct{
	Cache *genericstruct.NodeCache
	Ptrs  [Steps]int64
}
func (k *KeySearcher) Steps(off int64, key []byte) error {
	b,err := k.Cache.Get(off)
	if err!=nil { return err }
	node := b.(*Node)
	i := Steps-1
	k.Ptrs[i] = off
	for {
		next := node.Head.Nexts[i]
		
		// While next node is NULL, do:
		for next==0 && 0<i {
			// Try again with lower Level
			k.Ptrs[i-1] = k.Ptrs[i]
			i--
			next = node.Head.Nexts[i]
		}
		if next==0 { break }
		
		nb,err := k.Cache.Get(next)
		if err!=nil { return err }
		nnode := nb.(*Node)
		num := bytes.Compare(nnode.Key,key)
		
		// if nnode.Key < key, then:
		if num<0 {
			// Skip to next element.
			k.Ptrs[i] = next
			node = nnode
			continue
		}
		
		// if key < nnode.Key, then:
		if 0<num && 0<i {
			// Try again with lower Level
			k.Ptrs[i-1] = k.Ptrs[i]
			i--
			continue
		}
		
		// nnode.Key == key or i == 0
		break
	}
	
	// Fill the all the Levels.
	for 0<i {
		k.Ptrs[i-1] = k.Ptrs[i]
		i--
	}
	return nil
}

