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

import "math/rand"
import "github.com/maxymania/gobase/genericstruct"

func randomLevel() int {
	levelval := rand.Int31()
	
	// 0  = 50%
	// 1  = 25%
	// 2  = 12.5%
	// 3  = 6.25%
	// 4  = 3.125%
	// 5  = 1.5625%
	// 6  = 0.78125%
	// ......
	for i := 0 ; i<Steps ; i++ {
		if ((levelval>>uint(i))&1)==0 { return i }
	}
	return Steps-1
}

func InsertionAlgorithmV1(nc *genericstruct.NodeCache,off int64,key []byte, value int64) error {
	ks := KeySearcher{Cache:nc}
	err := ks.Steps(off,key)
	if err!=nil { return err }
	_,ok,err := ks.Found(key)
	if err!=nil { return err }
	if ok { return EExists }
	
	level := randomLevel()
	_,err = ks.Insert(key,value,level)
	return err
}

