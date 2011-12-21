// Copyright 2010-2011 James Roper
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package de.undercouch.bson4jackson.io

import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream

/**
 * Input stream that bounds an underlying input stream to a particular size.
 * This class is not threadsafe.
 * @author James Roper
 *
 * Wraps another stream by this one
 * @param in the stream to wrap
 * @param size the stream's size
 */
class BoundedInputStream(in:InputStream,size:Int) extends FilterInputStream(in) {
  private var count = 0
  private var eof = false
  private var mark:Int = _

  @throws(classOf[IOException])
  override def read():Int = synchronized {
    if (!eof && count < size) {
      val read = super.read()
      if (read == -1) {
        eof = true
      } else {
        count += 1
      }
      read
    }else{
      -1
    }
  }

  @throws(classOf[IOException])
  override def read(b:Array[Byte]):Int = {
    read(b, 0, b.length)
  }

  @throws(classOf[IOException])
  override def read(b:Array[Byte],off:Int,len:Int):Int = {
    if (eof || count >= size) {
      -1
    } else {
      // Bound length by what's remaining
      val l = math.min(len, size - count)
      val read = super.read(b, off, l)
      if (read == -1) {
        eof = true
      } else {
        count += l
      }
      read
    }
  }

  @throws(classOf[IOException])
  override def skip(n:Long):Long = {
    val m = math.min(n, size - count)
    val skipped = super.skip(m)
    count += skipped.asInstanceOf[Int]
    skipped
  }

  @throws(classOf[IOException])
  override def available():Int = {
    math.min(super.available(), size - count)
  }

  override def mark(readlimit:Int) {
    mark = count
    super.mark(readlimit)
  }

  @throws(classOf[IOException])
  override def reset(){
    if (super.markSupported()) {
      count = mark
    }
    super.reset()
  }
}
