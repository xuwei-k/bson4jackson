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
package de.undercouch.bson4jackson

import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.ArrayDeque
import java.util.Date
import java.util.Deque
import java.util.LinkedHashMap
import java.util.Map
import java.util.UUID
import java.util.regex.Pattern
import org.codehaus.jackson.Base64Variant
import org.codehaus.jackson.JsonLocation
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonStreamContext
import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.ObjectCodec
import org.codehaus.jackson.impl.JsonParserMinimalBase
import org.codehaus.jackson.`type`.TypeReference
import de.undercouch.bson4jackson.io.BoundedInputStream
import de.undercouch.bson4jackson.io.ByteOrderUtil
import de.undercouch.bson4jackson.io.CountingInputStream
import de.undercouch.bson4jackson.io.LittleEndianInputStream
import de.undercouch.bson4jackson.io.StaticBufferedInputStream
import de.undercouch.bson4jackson.types.JavaScript
import de.undercouch.bson4jackson.types.ObjectId
import de.undercouch.bson4jackson.types.Symbol
import de.undercouch.bson4jackson.types.Timestamp

/**
 * Reads a BSON document from the provided input stream
 * @author Michel Kraemer
 */
object BsonParser {

  /**
   * Defines toggable features
   */
  final object Feature {
    /**
     * Honors the document length field when parsing, useful for when
     * reading from streams that may contain other content after the
     * document that will be read by something else.
     */
    final val HONOR_DOCUMENT_LENGTH: = null
  }

  final class Feature {
    /**
     * @return the bit mask that identifies this feature
     */
    def getMask: Int = {
      return (1 << ordinal)
    }
  }

  /**
   * Specifies what the parser is currently parsing (field name or value) or
   * if it is done with the current element
   */
  private final object State {
    final val FIELDNAME: = null
    final val VALUE: = null
    final val DONE: = null
  }

  /**
   * Information about the element currently begin parsed
   */
  private class Context {
    def this(array: Boolean) {
      this ()
      this.array = array
    }

    def reset: Unit = {
      `type` = 0
      fieldName = null
      value = null
      state = State.FIELDNAME
    }

    /**
     * True if the document currently being parsed is an array
     */
    private[bson4jackson] final val array: Boolean = false
    /**
     * The bson type of the current element
     */
    private[bson4jackson] var `type`: Byte = 0
    /**
     * The field name of the current element
     */
    private[bson4jackson] var fieldName: Nothing = null
    /**
     * The value of the current element
     */
    private[bson4jackson] var value: Nothing = null
    /**
     * The parsing state of the current token
     */
    private[bson4jackson] var state: BsonParser.State = State.FIELDNAME
  }

  /**
   * Extends {@link JsonLocation} to offer a specialized string representation
   */
  private object BsonLocation {
    private final val serialVersionUID: Long = -5441597278886285168L
  }

  private class BsonLocation extends JsonLocation {
    def this(srcRef: Nothing, totalBytes: Long) {
      this ()
      `super`(srcRef, totalBytes, -1, -1, -1)
    }

    @Override override def toString: Nothing = {
      var sb: Nothing = new Nothing(80)
      sb.append("[Source: ")
      if (getSourceRef == null) {
        sb.append("UNKNOWN")
      }
      else {
        sb.append(getSourceRef.toString)
      }
      sb.append("; pos: ")
      sb.append(getByteOffset)
      sb.append(']')
      return sb.toString
    }
  }

}

class BsonParser extends JsonParserMinimalBase {
  /**
   * Constructs a new parser
   * @param jsonFeatures bit flag composed of bits that indicate which
   * { @link org.codehaus.jackson.JsonParser.Feature}s are enabled.
   * @param bsonFeatures bit flag composed of bits that indicate which
   * { @link Feature}s are enabled.
   * @param in the input stream to parse.
   */
  def this(jsonFeatures: Int, bsonFeatures: Int, in: Nothing) {
    this ()
    `super`(jsonFeatures)
    _bsonFeatures = bsonFeatures
    _rawInputStream = in
    if (!isEnabled(Feature.HONOR_DOCUMENT_LENGTH)) {
      if (!(in.isInstanceOf[Nothing])) {
        in = new StaticBufferedInputStream(in)
      }
      _counter = new CountingInputStream(in)
      _in = new LittleEndianInputStream(_counter)
    }
  }

  /**
   * Checks if a generator feature is enabled
   * @param f the feature
   * @return true if the given feature is enabled
   */
  protected def isEnabled(f: BsonParser.Feature): Boolean = {
    return (_bsonFeatures & f.getMask) != 0
  }

  @Override def getCodec: ObjectCodec = {
    return _codec
  }

  @Override def setCodec(c: ObjectCodec): Unit = {
    _codec = c
  }

  @Override def close: Unit = {
    if (isEnabled(JsonParser.Feature.AUTO_CLOSE_SOURCE)) {
      _in.close
    }
    _closed = true
  }

  @Override def nextToken: JsonToken = {
    var ctx: BsonParser.Context = _contexts.peek
    if (_currToken == null && ctx == null) {
      _currToken = handleNewDocument(false)
    }
    else {
      _tokenPos = _counter.getPosition
      if (ctx == null) {
        if (_currToken eq JsonToken.END_OBJECT) {
          return null
        }
        throw new JsonParseException("Found element outside the document", getTokenLocation)
      }
      if (ctx.state eq State.DONE) {
        ctx.reset
      }
      var readValue: Boolean = true
      if (ctx.state eq State.FIELDNAME) {
        readValue = false
        while (true) {
          ctx.`type` = _in.readByte
          if (ctx.`type` == BsonConstants.TYPE_END) {
            _currToken = (if (ctx.array) JsonToken.END_ARRAY else JsonToken.END_OBJECT)
            _contexts.pop
          }
          else if (ctx.`type` == BsonConstants.TYPE_UNDEFINED) {
            skipCString
            continue //todo: continue is not supported
          }
          else {
            ctx.state = State.VALUE
            _currToken = JsonToken.FIELD_NAME
            if (ctx.array) {
              readValue = true
              skipCString
              ctx.fieldName = null
            }
            else {
              ctx.fieldName = readCString
            }
          }
          break //todo: break is not supported
        }
      }
      if (readValue) {
        ctx.`type` match {
          case BsonConstants.TYPE_DOUBLE =>
            ctx.value = _in.readDouble
            _currToken = JsonToken.VALUE_NUMBER_FLOAT
            break //todo: break is not supported
          case BsonConstants.TYPE_STRING =>
            ctx.value = readString
            _currToken = JsonToken.VALUE_STRING
            break //todo: break is not supported
          case BsonConstants.TYPE_DOCUMENT =>
            _currToken = handleNewDocument(false)
            break //todo: break is not supported
          case BsonConstants.TYPE_ARRAY =>
            _currToken = handleNewDocument(true)
            break //todo: break is not supported
          case BsonConstants.TYPE_BINARY =>
            _currToken = handleBinary
            break //todo: break is not supported
          case BsonConstants.TYPE_OBJECTID =>
            ctx.value = readObjectId
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
            break //todo: break is not supported
          case BsonConstants.TYPE_BOOLEAN =>
            var b: Boolean = _in.readBoolean
            ctx.value = b
            _currToken = (if (b) JsonToken.VALUE_TRUE else JsonToken.VALUE_FALSE)
            break //todo: break is not supported
          case BsonConstants.TYPE_DATETIME =>
            ctx.value = new Nothing(_in.readLong)
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
            break //todo: break is not supported
          case BsonConstants.TYPE_NULL =>
            _currToken = JsonToken.VALUE_NULL
            break //todo: break is not supported
          case BsonConstants.TYPE_REGEX =>
            _currToken = handleRegEx
            break //todo: break is not supported
          case BsonConstants.TYPE_DBPOINTER =>
            _currToken = handleDBPointer
            break //todo: break is not supported
          case BsonConstants.TYPE_JAVASCRIPT =>
            ctx.value = new JavaScript(readString)
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
            break //todo: break is not supported
          case BsonConstants.TYPE_SYMBOL =>
            ctx.value = readSymbol
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
            break //todo: break is not supported
          case BsonConstants.TYPE_JAVASCRIPT_WITH_SCOPE =>
            _currToken = handleJavascriptWithScope
            break //todo: break is not supported
          case BsonConstants.TYPE_INT32 =>
            ctx.value = _in.readInt
            _currToken = JsonToken.VALUE_NUMBER_INT
            break //todo: break is not supported
          case BsonConstants.TYPE_TIMESTAMP =>
            ctx.value = readTimestamp
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
            break //todo: break is not supported
          case BsonConstants.TYPE_INT64 =>
            ctx.value = _in.readLong
            _currToken = JsonToken.VALUE_NUMBER_INT
            break //todo: break is not supported
          case BsonConstants.TYPE_MINKEY =>
            ctx.value = "MinKey"
            _currToken = JsonToken.VALUE_STRING
            break //todo: break is not supported
          case BsonConstants.TYPE_MAXKEY =>
            ctx.value = "MaxKey"
            _currToken = JsonToken.VALUE_STRING
            break //todo: break is not supported
          case _ =>
            throw new JsonParseException("Unknown element type " + ctx.`type`, getTokenLocation)
        }
        ctx.state = State.DONE
      }
    }
    return _currToken
  }

  /**
   * Can be called when a new embedded document is found. Reads the
   * document's header and creates a new context on the stack.
   * @param array true if the document is an embedded array
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleNewDocument(array: Boolean): JsonToken = {
    if (_in == null) {
      var buf: Array[Byte] = new Array[Byte](Integer.SIZE / Byte.SIZE)
      var len: Int = 0
      while (len < buf.length) {
        var l: Int = _rawInputStream.read(buf, len, buf.length - len)
        if (l == -1) {
          throw new Nothing("Not enough bytes for length of document")
        }
        len += l
      }
      var documentLength: Int = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
      var in: Nothing = new BoundedInputStream(_rawInputStream, documentLength - buf.length)
      if (!(_rawInputStream.isInstanceOf[Nothing])) {
        in = new StaticBufferedInputStream(in)
      }
      _counter = new CountingInputStream(in)
      _in = new LittleEndianInputStream(_counter)
    }
    else {
      _in.readInt
    }
    _contexts.push(new BsonParser.Context(array))
    return (if (array) JsonToken.START_ARRAY else JsonToken.START_OBJECT)
  }

  /**
   * Reads binary data from the input stream
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleBinary: JsonToken = {
    var size: Int = _in.readInt
    var subtype: Byte = _in.readByte
    var ctx: BsonParser.Context = getContext
    subtype match {
      case BsonConstants.SUBTYPE_BINARY_OLD =>
        var size2: Int = _in.readInt
        var buf2: Array[Byte] = new Array[Byte](size2)
        _in.readFully(buf2)
        ctx.value = buf2
        break //todo: break is not supported
      case BsonConstants.SUBTYPE_UUID =>
        var l1: Long = _in.readLong
        var l2: Long = _in.readLong
        ctx.value = new Nothing(l1, l2)
        break //todo: break is not supported
      case _ =>
        var buf: Array[Byte] = new Array[Byte](size)
        _in.readFully(buf)
        ctx.value = buf
        break //todo: break is not supported
    }
    return JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Converts a BSON regex pattern string to a combined value of Java flags that
   * can be used in {@link Pattern#compile(String, int)}
   * @param pattern the regex pattern string
   * @return the Java flags
   * @throws JsonParseException if the pattern string contains a unsupported flag
   */
  protected def regexStrToFlags(pattern: Nothing): Int = {
    var flags: Int = 0
    {
      var i: Int = 0
      while (i < pattern.length) {
        {
          var c: Char = pattern.charAt(i)
          c match {
            case 'i' =>
              flags |= Pattern.CASE_INSENSITIVE
              break //todo: break is not supported
            case 'm' =>
              flags |= Pattern.MULTILINE
              break //todo: break is not supported
            case 's' =>
              flags |= Pattern.DOTALL
              break //todo: break is not supported
            case 'u' =>
              flags |= Pattern.UNICODE_CASE
              break //todo: break is not supported
            case 'l' =>
            case 'x' =>
              break //todo: break is not supported
            case _ =>
              throw new JsonParseException("Invalid regex", getTokenLocation)
          }
        }
        ({
          i += 1; i - 1
        })
      }
    }
    return flags
  }

  /**
   * Reads and compiles a regular expression
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleRegEx: JsonToken = {
    var regex: Nothing = readCString
    var pattern: Nothing = readCString
    getContext.value = Pattern.compile(regex, regexStrToFlags(pattern))
    return JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Reads a DBPointer from the stream
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleDBPointer: JsonToken = {
    var pointer: Nothing = new Nothing
    pointer.put("$ns", readString)
    pointer.put("$id", readObjectId)
    getContext.value = pointer
    return JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Can be called when embedded javascript code with scope is found. Reads
   * the code and the embedded document.
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleJavascriptWithScope: JsonToken = {
    _in.readInt
    var code: Nothing = readString
    var doc: Nothing = readDocument
    getContext.value = new JavaScript(code, doc)
    return JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * @return a null-terminated string read from the input stream
   * @throws IOException if the string could not be read
   */
  protected def readCString: Nothing = {
    return _in.readUTF(-1)
  }

  /**
   * Skips over a null-terminated string in the input stream
   * @throws IOException if an I/O error occurs
   */
  protected def skipCString: Unit = {
    while (_in.readByte != 0)
  }

  /**
   * Reads a string that consists of a integer denoting the number of bytes,
   * the bytes (including a terminating 0 byte)
   * @return the string
   * @throws IOException if the string could not be read
   */
  protected def readString: Nothing = {
    var bytes: Int = _in.readInt
    if (bytes <= 0) {
      throw new Nothing("Invalid number of string bytes")
    }
    var s: Nothing = null
    if (bytes > 1) {
      s = _in.readUTF(bytes - 1)
    }
    else {
      s = ""
    }
    _in.readByte
    return s
  }

  /**
   * Reads a symbol object from the input stream
   * @return the symbol
   * @throws IOException if the symbol could not be read
   */
  protected def readSymbol: Symbol = {
    return new Symbol(readString)
  }

  /**
   * Reads a timestamp object from the input stream
   * @return the timestamp
   * @throws IOException if the timestamp could not be read
   */
  protected def readTimestamp: Timestamp = {
    var inc: Int = _in.readInt
    var time: Int = _in.readInt
    return new Timestamp(time, inc)
  }

  /**
   * Reads a ObjectID from the input stream
   * @return the ObjectID
   * @throws IOException if the ObjectID could not be read
   */
  protected def readObjectId: ObjectId = {
    var time: Int = ByteOrderUtil.flip(_in.readInt)
    var machine: Int = ByteOrderUtil.flip(_in.readInt)
    var inc: Int = ByteOrderUtil.flip(_in.readInt)
    return new ObjectId(time, machine, inc)
  }

  /**
   * Fully reads an embedded document, reusing this parser
   * @return the parsed document
   * @throws IOException if the document could not be read
   */
  protected def readDocument: Nothing = {
    var codec: ObjectCodec = getCodec
    if (codec == null) {
      throw new Nothing("Could not parse embedded document " + "because BSON parser has no codec")
    }
    _currToken = handleNewDocument(false)
    return codec.readValue(this, new TypeReference[Nothing] {
    })
  }

  /**
   * @return the context of the current element
   * @throws IOException if there is no context
   */
  protected def getContext: BsonParser.Context = {
    var ctx: BsonParser.Context = _contexts.peek
    if (ctx == null) {
      throw new Nothing("Context unknown")
    }
    return ctx
  }

  @Override def isClosed: Boolean = {
    return _closed
  }

  @Override def getCurrentName: Nothing = {
    var ctx: BsonParser.Context = _contexts.peek
    if (ctx == null) {
      return null
    }
    return ctx.fieldName
  }

  @Override def getParsingContext: JsonStreamContext = {
    return null
  }

  @Override def getTokenLocation: JsonLocation = {
    return new BsonParser.BsonLocation(_in, _tokenPos)
  }

  @Override def getCurrentLocation: JsonLocation = {
    return new BsonParser.BsonLocation(_in, _counter.getPosition)
  }

  @Override def getText: Nothing = {
    var ctx: BsonParser.Context = _contexts.peek
    if (ctx == null || ctx.state eq State.FIELDNAME) {
      return null
    }
    if (ctx.state eq State.VALUE) {
      return ctx.fieldName
    }
    return ctx.value.asInstanceOf[Nothing]
  }

  @Override def getTextCharacters: Array[Char] = {
    return getText.toCharArray
  }

  @Override def getTextLength: Int = {
    return getText.length
  }

  @Override def getTextOffset: Int = {
    return 0
  }

  @Override def hasTextCharacters: Boolean = {
    return false
  }

  @Override def getNumberValue: Nothing = {
    return getContext.value.asInstanceOf[Nothing]
  }

  @Override def getNumberType: JsonParser.NumberType = {
    var ctx: BsonParser.Context = _contexts.peek
    if (ctx == null) {
      return null
    }
    if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.INT
    }
    else if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.LONG
    }
    else if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.BIG_INTEGER
    }
    else if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.FLOAT
    }
    else if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.DOUBLE
    }
    else if (ctx.value.isInstanceOf[Nothing]) {
      return NumberType.BIG_DECIMAL
    }
    return null
  }

  @Override def getIntValue: Int = {
    return (getContext.value.asInstanceOf[Nothing]).intValue
  }

  @Override def getLongValue: Long = {
    return (getContext.value.asInstanceOf[Nothing]).longValue
  }

  @Override def getBigIntegerValue: Nothing = {
    var n: Nothing = getNumberValue
    if (n == null) {
      return null
    }
    if (n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing]) {
      return BigInteger.valueOf(n.longValue)
    }
    else if (n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing]) {
      return BigDecimal.valueOf(n.doubleValue).toBigInteger
    }
    return new Nothing(n.toString)
  }

  @Override def getFloatValue: Float = {
    return (getContext.value.asInstanceOf[Nothing]).floatValue
  }

  @Override def getDoubleValue: Double = {
    return (getContext.value.asInstanceOf[Nothing]).doubleValue
  }

  @Override def getDecimalValue: Nothing = {
    var n: Nothing = getNumberValue
    if (n == null) {
      return null
    }
    if (n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing]) {
      return BigDecimal.valueOf(n.longValue)
    }
    else if (n.isInstanceOf[Nothing] || n.isInstanceOf[Nothing]) {
      return BigDecimal.valueOf(n.doubleValue)
    }
    return new Nothing(n.toString)
  }

  @Override def getBinaryValue(b64variant: Base64Variant): Array[Byte] = {
    return getText.getBytes
  }

  @Override override def getEmbeddedObject: Nothing = {
    var ctx: BsonParser.Context = _contexts.peek
    return (if (ctx != null) ctx.value else null)
  }

  @Override protected def _handleEOF: Unit = {
    _reportInvalidEOF
  }

  /**
   * The features for this parser
   */
  private var _bsonFeatures: Int = 0
  /**
   * The input stream to read from
   */
  private var _in: LittleEndianInputStream = null
  /**
   * Counts the number of bytes read from {@link #_in}
   */
  private var _counter: CountingInputStream = null
  /**
   * The raw input stream passed in
   */
  private var _rawInputStream: Nothing = null
  /**
   * True if the parser has been closed
   */
  private var _closed: Boolean = false
  /**
   * The ObjectCodec used to parse the Bson object(s)
   */
  private var _codec: ObjectCodec = null
  /**
   * The position of the current token
   */
  private var _tokenPos: Int = 0
  /**
   * A stack of {@link Context} objects describing the current
   * parser state.
   */
  private var _contexts: Nothing = new Nothing
}