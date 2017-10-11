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


package genericstruct

import "github.com/maxymania/gobase/dataman"
import "github.com/hashicorp/golang-lru/simplelru"
import "github.com/valyala/bytebufferpool"
import "github.com/cznic/file"

func expand(i []byte,n int) []byte {
	if cap(i)<n { return make([]byte,n) }
	return i[:n]
}

type Block interface{
	Load(buf *bytebufferpool.ByteBuffer)
	Store(buf *bytebufferpool.ByteBuffer)
	Dirty() bool
}

type NodeMaster struct{
	Factory func() Block
	pool    bytebufferpool.Pool
}

type NodeCache struct{
	master *NodeMaster
	dman   dataman.DataManager
	cache  *simplelru.LRU
	rdonly bool
}

func (m *NodeMaster) Open(dman dataman.DataManager, rdonly bool) *NodeCache {
	c := new(NodeCache)
	
	c.master = m
	c.dman   = dman
	c.rdonly = rdonly
	lru,err := simplelru.NewLRU(1024,c.evict)
	if err!=nil { panic(err) }
	c.cache  = lru
	
	return c
}
func (c *NodeCache) evict(key interface{}, value interface{}) {
	if c.rdonly { return } // Do nothing
	buf := c.master.pool.Get()
	defer c.master.pool.Put(buf)
	off := key.(int64)
	value.(Block).Store(buf)
	
	size,err := c.dman.UsableSize(off)
	if err!=nil { return } // Error.
	
	if int64(len(buf.B))<size { return } // Error.
	
	c.dman.RollbackFile().WriteAt(buf.B,off)
}
func (c *NodeCache) file() file.File {
	if c.rdonly {
		return c.dman.DirectFile()
	} else {
		return c.dman.RollbackFile()
	}
}
func (c *NodeCache) Get(off int64) (Block,error) {
	b,ok := c.cache.Get(off)
	if ok { return b.(Block),nil }
	
	size,err := c.dman.UsableSize(off)
	if err!=nil { return nil,err }
	buf := c.master.pool.Get()
	defer c.master.pool.Put(buf)
	buf.B = expand(buf.B,int(size))
	_,err = c.file().ReadAt(buf.B,off)
	if err!=nil { c.master.pool.Put(buf); return nil,err }
	block := c.master.Factory()
	block.Load(buf)
	
	c.cache.Add(off,block)
	return block,nil
}
func (c *NodeCache) GetFromCache(off int64) (Block,bool) {
	b,ok := c.cache.Get(off)
	if ok { return b.(Block),true }
	return nil,false
}

