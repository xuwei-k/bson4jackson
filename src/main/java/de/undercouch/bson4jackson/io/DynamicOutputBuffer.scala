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

import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.CharBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.nio.charset.CharacterCodingException
import java.nio.charset.Charset
import java.nio.charset.CharsetEncoder
import java.nio.charset.CoderResult
import java.util.ArrayList
import java.util.LinkedList
import java.util.{List => JList}
import java.util.Queue

/**
 * <p>A random-access buffer that resizes itself. This buffer differentiates
 * from {@link java.io.ByteArrayOutputStream} in the following points:</p>
 * <ul>
 * <li>It allows specifying the byte order.</li>
 * <li>It assigns several internal buffers instead of one and therefore
 * saves garbage collection cycles.</li>
 * <li>It is able to flush some of its internal buffers to an output stream
 * or to a writable channel.</li>
 * </ul>
 * <p>The buffer has an initial size. This is also the size of each internal
 * buffer, so if a new buffer has to be allocated it will take exactly
 * that many bytes.</p>
 * <p>By calling {@link #flushTo(OutputStream)} or {@link #flushTo(WritableByteChannel)}
 * some of this buffer's internal buffers are flushed and then deallocated. The
 * buffer maintains an internal counter for all flushed buffers. This allows the
 * {@link #writeTo(OutputStream)} and {@link #writeTo(WritableByteChannel)}
 * methods to only write non-flushed buffers. So, this class can be used for
 * streaming by flushing internal buffers from time to time and at the end
 * writing the rest:
 * <pre>
 * ...
 * buf.flushTo(out);
 * ...
 * buf.flushTo(out);
 * ...
 * buf.flushTo(out);
 * ...
 * buf.writeTo(out);</pre>
 * </p>
 * <p>If flushing is never used a single call to one of the <code>writeTo</code>
 * methods is enough to write the whole buffer.</p>
 * <p>Once the buffer has been written to an output stream or channel, putting
 * elements into it is not possible anymore and will lead to an
 * {@link java.lang.IndexOutOfBoundsException}.</p>
 * @author Michel Kraemer
 */
object DynamicOutputBuffer {
  /**
   * The default byte order if nothing is specified
   */
  final val DEFAULT_BYTE_ORDER = ByteOrder.BIG_ENDIAN
  /**
   * A unique key to make the first buffer re-usable
   */
  private final val BUFFER_KEY = StaticBuffers.Key.BUFFER2
  /**
   * The default initial buffer size if nothing is specified
   */
  final val DEFAULT_BUFFER_SIZE = math.max(StaticBuffers.GLOBAL_MIN_SIZE, 1024 * 8)
}

/**
 * Creates a dynamic buffer with the given byte order and
 * the given initial buffer size.
 * @param order the byte order
 * @param initialSize the initial buffer size
 */
class DynamicOutputBuffer(private var order:ByteOrder,private var initialSize:Int){

  locally{
    if (initialSize <= 0) {
      throw new IllegalArgumentException("Initial buffer size must be larger than 0")
    }
    clear()
  }

  /**
   * Creates a dynamic buffer with the given byte order and
   * a default initial buffer size of {@link #DEFAULT_BUFFER_SIZE} bytes.
   * @param order the byte order
   */
  def this(order: Nothing) {
    this(order, DEFAULT_BUFFER_SIZE)
  }

  /**
   * Creates a dynamic buffer with BIG_ENDIAN byte order and
   * the given initial buffer size.
   * @param initialSize the initial buffer size
   */
  def this(initialSize: Int) {
    this(DEFAULT_BYTE_ORDER, initialSize)
  }

  /**
   * Creates a dynamic buffer with BIG_ENDIAN byte order and
   * a default initial buffer size of {@link #DEFAULT_BUFFER_SIZE} bytes.
   */
  def this() {
    this(DEFAULT_BYTE_ORDER)
  }

  /**
   * Sets the number of buffers to save for reuse after they have been
   * invalidated by {@link #flushTo(OutputStream)} or {@link #flushTo(WritableByteChannel)}.
   * Invalidated buffers will be saved in an internal queue. When the buffer
   * needs a new internal buffer, it first attempts to reuse an existing one
   * before allocating a new one.
   * @param count the number of buffers to save for reuse
   */
  def setReuseBuffersCount(count: Int): Unit = {
    _reuseBuffersCount = count
    if (_buffersToReuse != null) {
      if (_reuseBuffersCount == 0) {
        _buffersToReuse = null
      }
      else {
        while (_reuseBuffersCount < _buffersToReuse.size) {
          _buffersToReuse.poll
        }
      }
    }
  }

  /**
   * Allocates a new buffer or attempts to reuse an existing one.
   * @return a new buffer with the current buffer size and the current byte order
   */
  private def allocateBuffer:ByteBuffer = {
    if (_buffersToReuse != null && !_buffersToReuse.isEmpty) {
      val bb = _buffersToReuse.poll
      bb.rewind
      bb.limit(bb.capacity)
      bb
    }else{
      val r = StaticBuffers.getInstance.byteBuffer(BUFFER_KEY, _bufferSize)
      r.limit(_bufferSize)
      r.order(_order)
    }
  }

  /**
   * Removes a buffer from the list of internal buffers and saves it for
   * reuse if this feature is enabled.
   * @param n the number of the buffer to remove
   */
  private def deallocateBuffer(n: Int): Unit = {
    val bb = _buffers.set(n, null)
    if (bb != null && _reuseBuffersCount > 0) {
      if (_buffersToReuse == null) {
        _buffersToReuse = new LinkedList[byteBuffer]() 
      }
      if (_reuseBuffersCount > _buffersToReuse.size) {
        _buffersToReuse.add(bb)
      }
    }
  }

  /**
   * Adds a new buffer to the list of internal buffers
   * @return the new buffer
   */
  private def addNewBuffer():ByteBuffer = {
    val bb = allocateBuffer
    _buffers.add(bb)
    return bb
  }

  /**
   * Gets the buffer that holds the byte at the given absolute position.
   * Automatically adds new internal buffers if the position lies outside
   * the current range of all internal buffers.
   * @param position the position
   * @return the buffer at the requested position
   */
  private def getBuffer(position: Int): Nothing = {
    val n: Int = position / _bufferSize
    while (n >= _buffers.size) {
      addNewBuffer
    }
    return _buffers.get(n)
  }

  /**
   * Adapts the buffer size so it is at least equal to the
   * given number of bytes. This method does not add new
   * internal buffers.
   * @param size the minimum buffer size
   */
  private def adaptSize(size: Int): Unit = {
    if (size > _size) {
      _size = size
    }
  }

  /**
   * @return the lazily created UTF-8 character set
   */
  private def getUTF8Charset: Nothing = {
    if (_utf8 == null) {
      _utf8 = Charset.forName("UTF-8")
    }
    return _utf8
  }

  /**
   * @return the lazily created UTF-8 encoder
   */
  private def getUTF8Encoder: Nothing = {
    if (_utf8Encoder == null) {
      _utf8Encoder = getUTF8Charset.newEncoder
    }
    return _utf8Encoder
  }

  /**
   * @return the current buffer size (changes dynamically)
   */
  def size: Int = {
    return _size
  }

  /**
   * Clear the buffer and reset size and write position
   */
  def clear: Unit = {
    if (_buffersToReuse != null && !_buffersToReuse.isEmpty) {
      StaticBuffers.getInstance.releaseByteBuffer(BUFFER_KEY, _buffersToReuse.peek)
    }
    else if (!_buffers.isEmpty) {
      StaticBuffers.getInstance.releaseByteBuffer(BUFFER_KEY, _buffers.get(0))
    }
    if (_buffersToReuse != null) {
      _buffersToReuse.clear
    }
    _buffers.clear
    _position = 0
    _flushPosition = 0
    _size = 0
  }

  /**
   * Puts a byte into the buffer at the current write position
   * and increases the write position accordingly.
   * @param b the byte to put
   */
  def putByte(b: Byte): Unit = {
    putByte(_position, b)
    ({
      _position += 1; _position - 1
    })
  }

  /**
   * Puts several bytes into the buffer at the given position
   * and increases the write position accordingly.
   * @param bs an array of bytes to put
   */
  def putBytes(bs: Byte*): Unit = {
    putBytes(_position, bs)
    _position += bs.length
  }

  /**
   * Puts a byte into the buffer at the given position. Does
   * not increase the write position.
   * @param pos the position where to put the byte
   * @param b the byte to put
   */
  def putByte(pos: Int, b: Byte): Unit = {
    adaptSize(pos + 1)
    val bb: Nothing = getBuffer(pos)
    val i: Int = pos % _bufferSize
    bb.put(i, b)
  }

  /**
   * Puts several bytes into the buffer at the given position.
   * Does not increase the write position.
   * @param pos the position where to put the bytes
   * @param bs an array of bytes to put
   */
  def putBytes(pos: Int, bs: Byte*): Unit = {
    adaptSize(pos + bs.length)
    val bb: Nothing = null
    var i: Int = _bufferSize
    for (b <- bs) {
      if (i == _bufferSize) {
        bb = getBuffer(pos)
        i = pos % _bufferSize
      }
      bb.put(i, b)
      ({
        i += 1; i - 1
      })
      ({
        pos += 1; pos - 1
      })
    }
  }

  /**
   * Puts a 32-bit integer into the buffer at the current write position
   * and increases write position accordingly.
   * @param i the integer to put
   */
  def putInt(i: Int): Unit = {
    putInt(_position, i)
    _position += 4
  }

  /**
   * Puts a 32-bit integer into the buffer at the given position. Does
   * not increase the write position.
   * @param pos the position where to put the integer
   * @param i the integer to put
   */
  def putInt(pos: Int, i: Int): Unit = {
    adaptSize(pos + 4)
    val bb: Nothing = getBuffer(pos)
    val index: Int = pos % _bufferSize
    if (bb.limit - index >= 4) {
      bb.putInt(index, i)
    }
    else {
      val b0: Byte = i.asInstanceOf[Byte]
      val b1: Byte = (i >> 8).asInstanceOf[Byte]
      val b2: Byte = (i >> 16).asInstanceOf[Byte]
      val b3: Byte = (i >> 24).asInstanceOf[Byte]
      if (_order eq ByteOrder.BIG_ENDIAN) {
        putBytes(pos, b3, b2, b1, b0)
      }
      else {
        putBytes(pos, b0, b1, b2, b3)
      }
    }
  }

  /**
   * Puts a 64-bit integer into the buffer at the current write position
   * and increases the write position accordingly.
   * @param l the 64-bit integer to put
   */
  def putLong(l: Long): Unit = {
    putLong(_position, l)
    _position += 8
  }

  /**
   * Puts a 64-bit integer into the buffer at the given position. Does
   * not increase the write position.
   * @param pos the position where to put the integer
   * @param l the 64-bit integer to put
   */
  def putLong(pos: Int, l: Long): Unit = {
    adaptSize(pos + 8)
    val bb: Nothing = getBuffer(pos)
    val index: Int = pos % _bufferSize
    if (bb.limit - index >= 8) {
      bb.putLong(index, l)
    }
    else {
      val b0: Byte = l.asInstanceOf[Byte]
      val b1: Byte = (l >> 8).asInstanceOf[Byte]
      val b2: Byte = (l >> 16).asInstanceOf[Byte]
      val b3: Byte = (l >> 24).asInstanceOf[Byte]
      val b4: Byte = (l >> 32).asInstanceOf[Byte]
      val b5: Byte = (l >> 40).asInstanceOf[Byte]
      val b6: Byte = (l >> 48).asInstanceOf[Byte]
      val b7: Byte = (l >> 56).asInstanceOf[Byte]
      if (_order eq ByteOrder.BIG_ENDIAN) {
        putBytes(pos, b7, b6, b5, b4, b3, b2, b1, b0)
      }
      else {
        putBytes(pos, b0, b1, b2, b3, b4, b5, b6, b7)
      }
    }
  }

  /**
   * Puts a 32-bit floating point number into the buffer at the current
   * write position and increases the write position accordingly.
   * @param f the float to put
   */
  def putFloat(f: Float): Unit = {
    putFloat(_position, f)
    _position += 4
  }

  /**
   * Puts a 32-bit floating point number into the buffer at the given
   * position. Does not increase the write position.
   * @param pos the position where to put the float
   * @param f the float to put
   */
  def putFloat(pos: Int, f: Float): Unit = {
    putInt(pos, Float.floatToRawIntBits(f))
  }

  /**
   * Puts a 64-bit floating point number into the buffer at the current
   * write position and increases the write position accordingly.
   * @param d the double to put
   */
  def putDouble(d: Double): Unit = {
    putDouble(_position, d)
    _position += 8
  }

  /**
   * Puts a 64-bit floating point number into the buffer at the given
   * position. Does not increase the write position.
   * @param pos the position where to put the double
   * @param d the double to put
   */
  def putDouble(pos: Int, d: Double): Unit = {
    putLong(pos, Double.doubleToRawLongBits(d))
  }

  /**
   * Puts a character sequence into the buffer at the current
   * write position and increases the write position accordingly.
   * @param s the character sequence to put
   */
  def putString(s: Nothing): Unit = {
    putString(_position, s)
    _position += (s.length * 2)
  }

  /**
   * Puts a character sequence into the buffer at the given
   * position. Does not increase the write position.
   * @param pos the position where to put the character sequence
   * @param s the character sequence to put
   */
  def putString(pos: Int, s: Nothing): Unit = {
    {
      var i: Int = 0
      while (i < s.length) {
        {
          val c: Char = s.charAt(i)
          val b0: Byte = c.asInstanceOf[Byte]
          val b1: Byte = (c >> 8).asInstanceOf[Byte]
          if (_order eq ByteOrder.BIG_ENDIAN) {
            putBytes(pos, b1, b0)
          }
          else {
            putBytes(pos, b0, b1)
          }
          pos += 2
        }
        ({
          i += 1; i - 1
        })
      }
    }
  }

  /**
   * Encodes the given string as UTF-8, puts it into the buffer
   * and increases the write position accordingly.
   * @param s the string to put
   * @return the number of UTF-8 bytes put
   */
  def putUTF8(s: Nothing): Int = {
    val written: Int = putUTF8(_position, s)
    _position += written
    return written
  }

  /**
   * Puts the given string as UTF-8 into the buffer at the
   * given position. This method does not increase the write position.
   * @param pos the position where to put the string
   * @param s the string to put
   * @return the number of UTF-8 bytes put
   */
  def putUTF8(pos: Int, s: Nothing): Int = {
    var minibb: Nothing = null
    val enc: Nothing = getUTF8Encoder
    val in: Nothing = CharBuffer.wrap(s)
    var pos2: Int = pos
    var bb: Nothing = getBuffer(pos2)
    var index: Int = pos2 % _bufferSize
    bb.position(index)
    while (in.remaining > 0) {
      var res: Nothing = enc.encode(in, bb, true)
      if (bb eq minibb) {
        bb.flip
        while (bb.remaining > 0) {
          putByte(pos2, bb.get)
          ({
            pos2 += 1; pos2 - 1
          })
        }
      }
      else {
        pos2 += bb.position - index
      }
      if (res.isOverflow) {
        if (bb.remaining > 0) {
          if (minibb == null) {
            minibb = ByteBuffer.allocate(4)
          }
          minibb.rewind
          bb = minibb
          index = 0
        }
        else {
          bb = getBuffer(pos2)
          index = pos2 % _bufferSize
          bb.position(index)
        }
      }
      else if (res.isError) {
        try {
          res.throwException
        }
        catch {
          case e: Nothing => {
            throw new Nothing("Could not encode string", e)
          }
        }
      }
    }
    adaptSize(pos2)
    return pos2 - pos
  }

  /**
   * Tries to copy as much bytes as possible from this buffer to
   * the given channel. See {@link #flushTo(WritableByteChannel)}
   * for further information.
   * @param out the output stream to write to
   * @throws IOException if the buffer could not be flushed
   */
  def flushTo(out: Nothing): Unit = {
    val n1: Int = _flushPosition / _bufferSize
    val n2: Int = _position / _bufferSize
    if (n1 < n2) {
      flushTo(Channels.newChannel(out))
    }
  }

  /**
   * Tries to copy as much bytes as possible from this buffer to
   * the given channel. This method always copies whole internal
   * buffers and deallocates them afterwards. It does not deallocate
   * the buffer the write position is currently pointing to nor does
   * it deallocate buffers following the write position. The method
   * increases an internal pointer so consecutive calls also copy
   * consecutive bytes.
   * @param out the channel to write to
   * @throws IOException if the buffer could not be flushed
   */
  def flushTo(out: Nothing): Unit = {
    var n1: Int = _flushPosition / _bufferSize
    val n2: Int = _position / _bufferSize
    while (n1 < n2) {
      var bb: Nothing = _buffers.get(n1)
      bb.rewind
      out.write(bb)
      deallocateBuffer(n1)
      _flushPosition += _bufferSize
      ({
        n1 += 1; n1 - 1
      })
    }
  }

  /**
   * Writes all non-flushed internal buffers to the given output
   * stream. If {@link #flushTo(OutputStream)} has not been called
   * before, this method writes the whole buffer to the output stream.
   * @param out the output stream to write to
   * @throws IOException if the buffer could not be written
   */
  def writeTo(out: Nothing): Unit = {
    writeTo(Channels.newChannel(out))
  }

  /**
   * Writes all non-flushed internal buffers to the given channel.
   * If {@link #flushTo(WritableByteChannel)} has not been called
   * before, this method writes the whole buffer to the channel.
   * @param out the channel to write to
   * @throws IOException if the buffer could not be written
   */
  def writeTo(out: Nothing): Unit = {
    var n1: Int = _flushPosition / _bufferSize
    var n2: Int = _buffers.size
    var toWrite: Int = _size - _flushPosition
    while (n1 < n2) {
      var curWrite: Int = Math.min(toWrite, _bufferSize)
      var bb: Nothing = _buffers.get(n1)
      bb.position(curWrite)
      bb.flip
      out.write(bb)
      ({
        n1 += 1; n1 - 1
      })
      toWrite -= curWrite
    }
  }

  /**
   * The byte order of this buffer
   */
  private final val _order: Nothing = null
  /**
   * The size of each internal buffer (also the initial buffer size)
   */
  private final val _bufferSize: Int = 0
  /**
   * The current write position
   */
  private var _position: Int = 0
  /**
   * The position of the first byte that has not been already
   * flushed. Any attempt to put something into the buffer at
   * a position before this first byte is invalid and causes
   * a {@link IndexOutOfBoundsException} to be thrown.
   */
  private var _flushPosition: Int = 0
  /**
   * The current buffer size (changes dynamically)
   */
  private var _size: Int = 0
  /**
   * A linked list of internal buffers
   */
  private var _buffers: Nothing = new Nothing(1)
  /**
   * The character set used in {@link #putUTF8(String)}. Will be
   * created lazily in {@link #getUTF8Charset()}
   */
  private var _utf8: Nothing = null
  /**
   * The encoder used in {@link #putUTF8(String)}. Will be created
   * lazily in {@link #getUTF8Encoder()}
   */
  private var _utf8Encoder: Nothing = null
  /**
   * A queue of buffers that have already been flushed and are
   * free to reuse.
   * @see #_reuseBuffersCount
   */
  private var _buffersToReuse: Nothing = null
  /**
   * The number of buffers to reuse
   * @see #_buffersToReuse
   */
  private var _reuseBuffersCount: Int = 0
}
