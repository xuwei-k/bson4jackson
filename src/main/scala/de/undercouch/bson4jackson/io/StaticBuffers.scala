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
package de.undercouch.bson4jackson.io

import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.nio.CharBuffer

/**
 * Keeps thread-local re-usable buffers. Each buffer is identified by a key.
 * This class is a singleton, whereas the reference to the instance is hold
 * in a {@link SoftReference} so buffers can be freed when they are not needed
 * anymore.
 * @see org.codehaus.jackson.util.BufferRecycler
 * @author Michel Kraemer
 */
object StaticBuffers {
  /**
   * @return a thread-local singleton instance of this class
   */
  def getInstance(): StaticBuffers = {
    val ref = _instance.get()
    var buf = if (ref == null) null else ref.get()
    if (buf == null) {
      buf = new StaticBuffers
      _instance.set(new SoftReference(buf))
    }
    buf
  }

  /**
   * All buffers have a minimum size of 64 kb
   */
  final val GLOBAL_MIN_SIZE = 1024 * 64
  /**
   * A thread-local soft reference to the singleton instance of this class
   */
  private final val _instance = new ThreadLocal[SoftReference[StaticBuffers]]()

  /**
   * Possible buffer keys
   */
  object Key extends Enumeration{
    val BUFFER0,BUFFER1,BUFFER2,BUFFER3,BUFFER4,BUFFER5,BUFFER6,BUFFER7,BUFFER8,BUFFER9 = Value
  }

}

/**
 * Hidden constructor
 */
class StaticBuffers private(){
  import StaticBuffers._

  /**
   * Creates or re-uses a {@link CharBuffer} that has a minimum size. Calling
   * this method multiple times with the same key will always return the
   * same buffer, as long as it has the minimum size and is marked to be
   * re-used. Buffers that are allowed to be re-used should be released using
   * {@link #releaseCharBuffer(Key, CharBuffer)}.
   * @param key the buffer's identifier
   * @param minSize the minimum size
   * @return the { @link CharBuffer} instance
   */
  def charBuffer(key: StaticBuffers.Key.Value, m: Int):CharBuffer = {
    val minSize = math.max(m, GLOBAL_MIN_SIZE)
    var r = _charBuffers(key.id)
    if (r == null || r.capacity < minSize) {
      r = CharBuffer.allocate(minSize)
    }
    else {
      _charBuffers(key.id) = null
      r.clear
    }
    r
  }

  /**
   * Marks a buffer a being re-usable.
   * @param key the buffer's key
   * @param buf the buffer
   */
  def releaseCharBuffer(key: StaticBuffers.Key.Value, buf: CharBuffer): Unit = {
    _charBuffers(key.id) = buf
  }

  /**
   * @see #charBuffer(Key, int)
   */
  def byteBuffer(key: StaticBuffers.Key.Value, m: Int):ByteBuffer = {
    val minSize = math.max(m, GLOBAL_MIN_SIZE)
    var r = _byteBuffers(key.id)
    if (r == null || r.capacity < minSize) {
      r = ByteBuffer.allocate(minSize)
    }
    else {
      _byteBuffers(key.id) = null
      r.clear
    }
    r
  }

  /**
   * @see #releaseCharBuffer(Key, CharBuffer)
   */
  def releaseByteBuffer(key: StaticBuffers.Key.Value, buf: ByteBuffer): Unit = {
    _byteBuffers(key.id) = buf
  }

  /**
   * Maps of already allocated re-usable buffers
   */
  private[this] var _byteBuffers = new Array[ByteBuffer](Key.values.size)
  private[this] var _charBuffers = new Array[CharBuffer](Key.values.size)
}
