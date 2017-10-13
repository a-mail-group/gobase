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
import "sync"



type BLManager struct{
	DM    *dataman.DataManagerLocked
	Off   int64
	mutex sync.Mutex
}
func (b *BLManager) AppendNodeAndConsume(obtain func(dm dataman.DataManager)(int64,error) ) (bool,error) {
	b.mutex.Lock(); defer b.mutex.Unlock()
	b.DM.Lock(); defer b.DM.Unlock()
	
	off,e := obtain(b.DM)
	if e!=nil || off==0 { return false,e }
	
	e = AddElementsAndFree(b.DM,b.Off,off)
	
	if e!=nil { return false,e }
	
	e = b.DM.Free(off)
	
	if e!=nil { return false,e }
	
	return true,b.DM.Commit()
}
func (b *BLManager) AppendNodeAndClear(other int64) error {
	b.mutex.Lock(); defer b.mutex.Unlock()
	b.DM.Lock(); defer b.DM.Unlock()
	
	e := AddElementsAndFree(b.DM,b.Off,other)
	if e!=nil { return e }
	
	_,e = b.DM.RollbackFile().WriteAt(make([]byte,16),other)
	
	if e!=nil { return e }
	
	return b.DM.Commit()
}
func (b *BLManager) FreeElements(maxElems int) error {
	// Read-Phase
	b.mutex.Lock(); defer b.mutex.Unlock()
	
	nhd,elems,err := IterateOverList(b.DM.DirectFile(),b.Off,maxElems)
	if err!=nil { return err }
	
	// Write Phase
	b.DM.Lock(); defer b.DM.Unlock()
	
	err = SetHead(b.DM.RollbackFile(),b.Off,nhd)
	if err!=nil { return err }
	
	for _,off := range elems {
		err = b.DM.Free(off)
		if err!=nil { return err }
	}
	
	return b.DM.Commit()
}
func (b *BLManager) FreeElementsEx(maxElems int, updFreed func(dataman.DataManager,int64) error ) error {
	// Read-Phase
	b.mutex.Lock(); defer b.mutex.Unlock()
	
	nhd,elems,err := IterateOverListEx(b.DM.DirectFile(),b.Off,maxElems)
	if err!=nil { return err }
	
	if len(elems)==0 { return nil }
	
	// Write Phase
	b.DM.Lock(); defer b.DM.Unlock()
	
	err = SetHead(b.DM.RollbackFile(),b.Off,nhd)
	if err!=nil { return err }
	
	freed := int64(0)
	for _,elem := range elems {
		err = b.DM.Free(elem.Off)
		if err!=nil { return err }
		freed+=int64(elem.Len+16)
	}
	
	err = updFreed(b.DM,freed)
	if err!=nil { return err }
	
	return b.DM.Commit()
}

