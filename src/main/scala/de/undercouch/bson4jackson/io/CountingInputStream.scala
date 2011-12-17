// Copyright 2010-2011 Michel Kraemer
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

package de.undercouch.bson4jackson.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads from another input stream, but counts the number of
 * bytes read or skipped (i.e. saves the current buffer position).
 * @author Michel Kraemer
 *
 * @see FilterInputStream#FilterInputStream(InputStream)
 */
class CountingInputStream(in:InputStream) extends FilterInputStream(in) {
  /**
   * The current buffer position
   */
  private var _pos:Int = _

  /**
   * @return the number of bytes read or skipped
   */
  def getPosition():Int = _pos

  @throws(classOf[IOException])
  override def read():Int = {
    val r = super.read()
    if (r > 0) {
      _pos += 1
    }
    r;
  }

  @throws(classOf[IOException])
  override def read(b:Array[Byte]):Int = {
    val r = super.read(b)
    if (r > 0) {
      _pos += r;
    }
    r;
  }

  @throws(classOf[IOException])
  override def read(b:Array[Byte], off:Int,len:Int):Int = {
    val r = super.read(b, off, len);
    if (r > 0) {
      _pos += r;
    }
    r;
  }

  @throws(classOf[IOException])
  override def skip(n:Long):Long = {
    val r = super.skip(n);
    if (r > 0) {
      _pos += r.asInstanceOf[Int]
    }
    r
  }
}
