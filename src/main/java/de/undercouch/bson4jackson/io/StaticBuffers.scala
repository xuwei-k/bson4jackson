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
  def getInstance: StaticBuffers = {
    var ref: Nothing = _instance.get
    var buf: StaticBuffers = (if (ref == null) null else ref.get)
    if (buf == null) {
      buf = new StaticBuffers
      _instance.set(new Nothing(buf))
    }
    return buf
  }

  /**
   * All buffers have a minimum size of 64 kb
   */
  final val GLOBAL_MIN_SIZE: Int = 1024 * 64
  /**
   * A thread-local soft reference to the singleton instance of this class
   */
  private final val _instance: Nothing = new Nothing

  /**
   * Possible buffer keys
   */
  final object Key {
    final val BUFFER0: = null
    final val BUFFER1: = null
    final val BUFFER2: = null
    final val BUFFER3: = null
    final val BUFFER4: = null
    final val BUFFER5: = null
    final val BUFFER6: = null
    final val BUFFER7: = null
    final val BUFFER8: = null
    final val BUFFER9: = null
  }

}

class StaticBuffers {
  /**
   * Hidden constructor
   */
  private def this() {
    this ()
  }

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
  def charBuffer(key: StaticBuffers.Key, minSize: Int): Nothing = {
    minSize = Math.max(minSize, GLOBAL_MIN_SIZE)
    var r: Nothing = _charBuffers(key.ordinal)
    if (r == null || r.capacity < minSize) {
      r = CharBuffer.allocate(minSize)
    }
    else {
      _charBuffers(key.ordinal) = null
      r.clear
    }
    return r
  }

  /**
   * Marks a buffer a being re-usable.
   * @param key the buffer's key
   * @param buf the buffer
   */
  def releaseCharBuffer(key: StaticBuffers.Key, buf: Nothing): Unit = {
    _charBuffers(key.ordinal) = buf
  }

  /**
   * @see #charBuffer(Key, int)
   */
  def byteBuffer(key: StaticBuffers.Key, minSize: Int): Nothing = {
    minSize = Math.max(minSize, GLOBAL_MIN_SIZE)
    var r: Nothing = _byteBuffers(key.ordinal)
    if (r == null || r.capacity < minSize) {
      r = ByteBuffer.allocate(minSize)
    }
    else {
      _byteBuffers(key.ordinal) = null
      r.clear
    }
    return r
  }

  /**
   * @see #releaseCharBuffer(Key, CharBuffer)
   */
  def releaseByteBuffer(key: StaticBuffers.Key, buf: Nothing): Unit = {
    _byteBuffers(key.ordinal) = buf
  }

  /**
   * Maps of already allocated re-usable buffers
   */
  private var _byteBuffers: Array[Nothing] = new Array[Nothing](Key.values.length)
  private var _charBuffers: Array[Nothing] = new Array[Nothing](Key.values.length)
}