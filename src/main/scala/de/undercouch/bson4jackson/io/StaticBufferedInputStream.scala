// Copyright 2010-2011 Michel Kraemer
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

object StaticBufferedInputStream{
  /**
   * A unique key for the re-usable buffer
   */
  private val BUFFER_KEY = StaticBuffers.Key.BUFFER1
}

/**
 * Extends {@link BufferedInputStream}, but uses a a re-usable static buffer
 * provided by {@link StaticBuffers} to achieve better performance.
 * @author Michel Kraemer
 */
class StaticBufferedInputStream(
    in:InputStream,size:Int
  )extends BufferedInputStream(in,1) {

  import StaticBufferedInputStream._

  /**
   * Provides re-usable buffers
   */
  private final val _staticBuffers = StaticBuffers.getInstance()

  /**
   * A re-usable buffer
   */
  private val _byteBuffer = _staticBuffers.byteBuffer(BUFFER_KEY, size)

  //replace buffer allocated by super constructor
  buf = _byteBuffer.array()

  /**
   * @see BufferedInputStream#BufferedInputStream(InputStream)
   */
  def this(in:InputStream) {
    this(in, 8192)
  }

  @throws(classOf[IOException])
  override def close(){
    _staticBuffers.releaseByteBuffer(BUFFER_KEY, _byteBuffer)
    super.close()
  }
}
