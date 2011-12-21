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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import scala.util.control.Breaks._

object LittleEndianInputStream{

  /**
   * A unique key for a buffer used in {@link #readUTF(DataInput, int)}
   */
  private final val UTF8_BUFFER = StaticBuffers.Key.BUFFER0;

  /**
   * The decoder used in {@link #readUTF(int)}. Will be created
   * lazily in {@link #getUTF8Decoder()}
   */
  private var _utf8Decoder:CharsetDecoder = _;

  /**
   * The character set used in {@link #readUTF(int)}. Will be
   * created lazily in {@link #getUTF8Charset()}
   */
  private var _utf8:Charset = _;


  /**
   * @return the lazily created UTF-8 character set
   */
  private def getUTF8Charset():Charset = {
    if (_utf8 == null) {
      _utf8 = Charset.forName("UTF-8");
    }
    _utf8;
  }

  /**
   * @return the lazily created UTF-8 decoder
   */
  private def getUTF8Decoder():CharsetDecoder = {
    if (_utf8Decoder == null) {
      _utf8Decoder = getUTF8Charset().newDecoder();
    }
    _utf8Decoder;
  }

}

/**
 * Works like {@link DataInputStream} but reads values using
 * little-endian encoding. Apart from that, it provides a method
 * that reads an UTF-8 encoded string without reading the number of
 * bytes from the input stream.
 * @author Michel Kraemer
 *
 * @see FilterInputStream#FilterInputStream(InputStream)
 */
class LittleEndianInputStream(i:InputStream) extends FilterInputStream(i) with DataInput {
  import LittleEndianInputStream._

  /**
   * A small buffer to speed up reading slightly
   */
  private val _rawBuf = new Array[Byte](8)

  /**
   * Wraps around {@link #_rawBuf}
   */
  private val _buf = ByteBuffer.wrap(_rawBuf).order(ByteOrder.LITTLE_ENDIAN);

  /**
   * A buffer that will lazily be initialized by {@link #readLine()}
   */
  private var _lineBuffer:CharBuffer = _

  /**
   * Used to create re-usable buffers
   */
  private final val _staticBuffers = StaticBuffers.getInstance();

  @throws(classOf[IOException])
  override def readFully(b:Array[Byte]){
    readFully(b, 0, b.length);
  }

  @throws(classOf[IOException])
  override def readFully(b:Array[Byte], o:Int, l:Int){
    var len = l
    var off = o
    while (len > 0) {
      val r = read(b, off, len);
      if (r < 0) {
        throw new EOFException();
      }
      len -= r;
      off += r;
    }
  }

  @throws(classOf[IOException])
  override def skipBytes(m:Int):Int = {
    var n = m
    var r = 0;
    breakable{
      while (n > 0) {
        val s = skip(n).asInstanceOf[Int]
        if (s <= 0) {
          break;
        }
        r += s;
        n -= s;
      }
    }
    r
  }

  @throws(classOf[IOException])
  override def readBoolean():Boolean = {
    (readByte() != 0);
  }

  @throws(classOf[IOException])
  override def readByte():Byte = {
    readUnsignedByte().asInstanceOf[Byte]
  }

  @throws(classOf[IOException])
  override def readUnsignedByte():Int = {
    val r = read();
    if (r < 0) {
      throw new EOFException();
    }
    r;
  }

  @throws(classOf[IOException])
  override def readShort():Short = {
    readUnsignedShort().asInstanceOf[Short]
  }

  @throws(classOf[IOException])
  override def readUnsignedShort():Int = {
    val r1 = readUnsignedByte();
    val r2 = readUnsignedByte();
    (r1 + (r2 << 8));
  }

  @throws(classOf[IOException])
  override def readChar():Char = {
    readUnsignedShort().asInstanceOf[Char]
  }

  @throws(classOf[IOException])
  override def readInt():Int = {
    readFully(_rawBuf, 0, 4);
    return _buf.getInt(0);
  }

  @throws(classOf[IOException])
  override def readLong():Long = {
    readFully(_rawBuf, 0, 8);
    return _buf.getLong(0);
  }

  @throws(classOf[IOException])
  override def readFloat():Float = {
    readFully(_rawBuf, 0, 4);
    return _buf.getFloat(0);
  }

  @throws(classOf[IOException])
  override def readDouble():Double = {
    readFully(_rawBuf, 0, 8);
    return _buf.getDouble(0);
  }

  @throws(classOf[IOException])
  override def readLine():String = {
    var bufSize = 0;
    if (_lineBuffer != null) {
      _lineBuffer.rewind();
      _lineBuffer.limit(_lineBuffer.capacity());
      bufSize = _lineBuffer.capacity();
    }

    var c = 0
    breakable{
      while (true) {
        c = read();
        if (c < 0 || c == '\n') {
          break;
        } else if (c == '\r') {
          val c2 = read();
          if (c2 != -1 && c2 != '\n') {
            if (!(in.isInstanceOf[PushbackInputStream])) {
              this.in = new PushbackInputStream(in);
            }
            in.asInstanceOf[PushbackInputStream].unread(c2);
          }
          break;
        } else {
          if (_lineBuffer == null || _lineBuffer.remaining() == 0) {
            val newBufSize = bufSize + 128;
            val newBuf = CharBuffer.allocate(newBufSize);
            if (_lineBuffer != null) {
              _lineBuffer.flip();
              newBuf.put(_lineBuffer);
            }
            _lineBuffer = newBuf;
            bufSize = newBufSize;
          }
          _lineBuffer.put(c.asInstanceOf[Char]);
        }
      }
    }
    if (c < 0 && (_lineBuffer == null || _lineBuffer.position() == 0)) {
      return null;
    }
    String.valueOf(_lineBuffer.array(), 0, _lineBuffer.position());
  }

  /**
   * <p>Forwards to {@link DataInputStream#readUTF(DataInput)} which expects
   * a short value at the beginning of the UTF-8 string that specifies
   * the number of bytes to read.</p>
   * <p>If the output stream does no include such a short value, use
   * {@link #readUTF(int)} to explicitly specify the number of bytes.</p>
   */
  @throws(classOf[IOException])
  override def readUTF():String = DataInputStream.readUTF(this);

  /**
   * Reads a modified UTF-8 string
   * @param len the number of bytes to read (please do not mix that up
   * with the number of characters!). If this is -1 then the method
   * will read bytes until the first one is zero (0x00). The zero
   * byte will not be included in the result string.
   * @return the UTF-8 string
   * @throws IOException if an I/O error occurs
   * @throws CharacterCodingException if an invalid UTF-8 character
   * has been read
   */
  @throws(classOf[IOException])
  def readUTF(len:Int):String = readUTF(this, len);

  /**
   * Reads a modified UTF-8 string from a DataInput object
   * @param input the DataInput object to read from
   * @param len the number of bytes to read (please do not mix that up
   * with the number of characters!). If this is -1 then the method
   * will read bytes until the first one is zero (0x00). The zero
   * byte will not be included in the result string.
   * @return the UTF-8 string
   * @throws IOException if an I/O error occurs
   * @throws CharacterCodingException if an invalid UTF-8 character
   * has been read
   */
  @throws(classOf[IOException])
  def readUTF(input:DataInput, l:Int):String = {
    var len = l
    val utf8buf = _staticBuffers.byteBuffer(UTF8_BUFFER, 1024 * 8);
    val rawUtf8Buf = utf8buf.array();

    val dec = getUTF8Decoder();
    val expectedLen =
      if(len > 0){
        ((dec.averageCharsPerByte() * len) + 1 ).asInstanceOf[Int]
      }else{
        1024
      }

    var cb = _staticBuffers.charBuffer(UTF8_BUFFER, expectedLen);
    try {
        while (len != 0 || utf8buf.position() > 0) {
          //read as much as possible
          if (len < 0) {
            //read until the first zero byte
            breakable{
              while (utf8buf.remaining() > 0) {
                val b = input.readByte();
                if (b == 0) {
                  len = 0;
                  break;
                }
                utf8buf.put(b);
              }
            }
            utf8buf.flip();
          } else if (len > 0) {
            val r = Math.min(len, utf8buf.remaining());
            input.readFully(rawUtf8Buf, utf8buf.position(), r);
            len -= r;
            utf8buf.limit(r);
            utf8buf.rewind();
          } else {
            utf8buf.flip();
          }

          //decode byte buffer
          val cr = dec.decode(utf8buf, cb, len == 0);
          if (cr.isUnderflow()) {
            //too few input bytes. move rest of the buffer
            //to the beginning and then try again
            utf8buf.compact();
          } else if (cr.isOverflow()) {
            //output buffer to small. enlarge buffer and try again
            utf8buf.compact();

            //create a new char buffer with the same key
            val newBuf = _staticBuffers.charBuffer(UTF8_BUFFER,cb.capacity() + 1024);

            cb.flip();
            newBuf.put(cb);
            cb = newBuf;
          } else if (cr.isError()) {
            cr.throwException();
          }
        }
    } finally {
      _staticBuffers.releaseCharBuffer(UTF8_BUFFER, cb);
      _staticBuffers.releaseByteBuffer(UTF8_BUFFER, utf8buf);
    }

    cb.flip();
    return cb.toString();
  }
}
