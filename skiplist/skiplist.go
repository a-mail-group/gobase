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
import "errors"

const Steps = 20

var NodeMaster = &genericstruct.NodeMaster{Factory:NodeConstructor}

//var EWrongSeek = errors.New("EWrongSeek")
var EExists = errors.New("EExists")

var EDisabled = errors.New("EDisabled")


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

// This object must be used with care - otherwise the Skiplist becomes out of order.
type KeySearcher struct{
	Cache *genericstruct.NodeCache
	Ptrs  [Steps]int64
	Jumps [Steps]int
}
func (k *KeySearcher) Steps(off int64, key []byte) error {
	b,err := k.Cache.Get(off)
	if err!=nil { return err }
	node := b.(*Node)
	for idx := range k.Jumps { k.Jumps[idx] = 0 }
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
			k.Jumps[i]++
			continue
		}
		
		// if key <= nnode.Key, then:
		if 0<=num && 0<i {
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

// Fast path towards finding.
func (k *KeySearcher) StepsFind(off int64, key []byte) (int64,*Node,error) {
	b,err := k.Cache.Get(off)
	if err!=nil { return 0,nil,err }
	node := b.(*Node)
	for idx := range k.Jumps { k.Jumps[idx] = 0 }
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
		if err!=nil { return 0,nil,err }
		nnode := nb.(*Node)
		num := bytes.Compare(nnode.Key,key)
		
		// if nnode.Key < key, then:
		if num<0 {
			// Skip to next element.
			k.Ptrs[i] = next
			node = nnode
			k.Jumps[i]++
			continue
		}
		// if nnode.Key == key, then return immediately.
		if num==0 {
			return next,node,nil
		}
		
		// if key <= nnode.Key, then:
		if 0<num && 0<i {
			// Try again with lower Level
			k.Ptrs[i-1] = k.Ptrs[i]
			i--
			continue
		}
		
		// nnode.Key == key or i == 0
		break
	}
	return 0,nil,nil
}


func (k *KeySearcher) foundRef(key []byte) (it int64,it2 *Node,ok bool,err error) {
	b,e := k.Cache.Get(k.Ptrs[0])
	if e!=nil { err = e; return }
	node := b.(*Node)
	nxt := node.Head.Nexts[0]
	if nxt==0 { return }
	b,e = k.Cache.Get(nxt)
	if e!=nil { err = e; return }
	node = b.(*Node)
	
	num := bytes.Compare(node.Key,key)  // num = node.Key-key : node.Key<key = num<0
	if num!=0 { return }
	it  = nxt
	it2 = node
	ok  = true
	return
}

func (k *KeySearcher) FoundNode(key []byte) (it *Node,ok bool,err error) {
	b,e := k.Cache.Get(k.Ptrs[0])
	if e!=nil { err = e; return }
	node := b.(*Node)
	nxt := node.Head.Nexts[0]
	if nxt==0 { return }
	b,e = k.Cache.Get(nxt)
	if e!=nil { err = e; return }
	node = b.(*Node)
	
	num := bytes.Compare(node.Key,key)  // num = node.Key-key : node.Key<key = num<0
	if num!=0 { return }
	it = node
	ok = true
	return
}

func (k *KeySearcher) Found(key []byte) (it int64,ok bool,err error) {
	b,e := k.Cache.Get(k.Ptrs[0])
	if e!=nil { err = e; return }
	node := b.(*Node)
	nxt := node.Head.Nexts[0]
	if nxt==0 { return }
	b,e = k.Cache.Get(nxt)
	if e!=nil { err = e; return }
	node = b.(*Node)
	
	num := bytes.Compare(node.Key,key)  // num = node.Key-key : node.Key<key = num<0
	if num!=0 { return }
	it = node.Head.Content
	ok = true
	return
}
/*
Inserts a key-value pair.

IMPORTANT: Must not be called, unless k.Steps() was called before with the same key,
and k.Found() was checked with the same key, that this one doesn't exist

	k.Steps(listHead,KEY)   // Perform the search
	_,ok,_ := k.Found(KEY)  // Check, if we found the key.
	
	// Insert the pair IF, AND ONLY IF, we didn't have the pair already.
	if !ok { k.Insert(KEY,VALUE,level) }
*/
func (k *KeySearcher) Insert(key []byte, value int64, level int) (int64,error) {
	if level<0 || level>=Steps { panic("level out of range") }
	noff,err := k.Cache.Set(&Node{Key:key,Head:NodeHead{Content:value}})
	if err!=nil { return 0,err }
	//if noff==0 { return 0,EDisabled } // Won't happen!!!
	
	rn,err := k.Cache.Get(noff)
	if err!=nil { return 0,err }
	node := rn.(*Node)
	node.Tainted = true
	
	// This loop will link at any requested level.
	i := level
	for {
		rpt,err := k.Cache.Get(k.Ptrs[i])
		if err!=nil { return 0,err }
		pt := rpt.(*Node)
		
		node.Head.Nexts[i] = pt.Head.Nexts[i]
		
		pt.Head.Nexts[i] = noff
		pt.Tainted = true
		
		if i==0 { break }
		i--
	}
	return noff,nil
}


