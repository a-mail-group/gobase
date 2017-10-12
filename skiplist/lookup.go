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

import "github.com/maxymania/gobase/genericstruct"



func LookupNode(nc *genericstruct.NodeCache,off int64,key []byte) (*Node,bool,error) {
	ks := KeySearcher{Cache:nc}
	_,node,err := ks.StepsFind(off,key)
	if err!=nil { return nil,false,err }
	return node,node!=nil,nil
}

func Lookup(nc *genericstruct.NodeCache,off int64,key []byte) (int64,bool,error) {
	ks := KeySearcher{Cache:nc}
	_,node,err := ks.StepsFind(off,key)
	if err!=nil { return 0,false,err }
	if node==nil { return 0,false,nil }
	return node.Head.Content,true,nil
}


func Delete(nc *genericstruct.NodeCache,off int64,key []byte) (bool,error) {
	ks := KeySearcher{Cache:nc}
	err := ks.Steps(off,key)
	if err!=nil { return false,err }
	ref,node,ok,err := ks.foundRef(key) // nc[ref] => node
	if err!=nil { return false,err }
	if !ok { return false,nil }
	// This loop untethers all Links to the current node.
	for i:=0 ; i<Steps ; i++ {
		nb,err := nc.Get(ks.Ptrs[i])
		if err!=nil { return false,err }
		nnode := nb.(*Node)
		if nnode.Head.Nexts[i]==ref {
			nnode.Head.Nexts[i] = node.Head.Nexts[i]
			nnode.Tainted = true
		}
	}
	nc.Flush() // Flush the cache.
	return true,nc.Delete(ref)
}
