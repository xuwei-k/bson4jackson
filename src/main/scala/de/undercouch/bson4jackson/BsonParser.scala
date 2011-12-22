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

import java.io.{BufferedInputStream,IOException,InputStream}
import java.math.{BigDecimal,BigInteger}
import java.nio.{ByteBuffer,ByteOrder}
import java.util.{
  ArrayDeque,Date,Deque,LinkedHashMap,UUID,Map => JMap
}
import java.util.regex.Pattern
import org.codehaus.jackson.impl.JsonParserMinimalBase
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.{
  Base64Variant,JsonLocation,JsonParseException,JsonParser,JsonStreamContext,JsonToken,ObjectCodec
}
import de.undercouch.bson4jackson.io.{
  BoundedInputStream,ByteOrderUtil,CountingInputStream,LittleEndianInputStream,StaticBufferedInputStream
}
import de.undercouch.bson4jackson.types.{
  JavaScript,ObjectId,Symbol,Timestamp
}
import java.lang.{
  Integer => JInteger,Byte => JByte,Long => JLong,Short => JShort,Double => JDouble,Float => JFloat,Boolean => JBoolean
}
import scala.util.control.Breaks._

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
    final val HONOR_DOCUMENT_LENGTH = new Feature("HONOR_DOCUMENT_LENGTH",0)
  }

  final class Feature private(name:String,ord:Int) extends java.lang.Enum[Feature](name,ord){
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
  object State extends Enumeration{
    val FIELDNAME,VALUE,DONE = Value
  }

  /**
   * Information about the element currently begin parsed
   *
   * @param True if the document currently being parsed is an array
   */
  class Context(val array:Boolean = false) {

    def reset: Unit = {
      `type` = 0
      fieldName = null
      value = null
      state = State.FIELDNAME
    }

    /**
     * The bson type of the current element
     */
    private[bson4jackson] var `type`: Byte = 0
    /**
     * The field name of the current element
     */
    private[bson4jackson] var fieldName: String = _
    /**
     * The value of the current element
     */
    private[bson4jackson] var value: AnyRef = _
    /**
     * The parsing state of the current token
     */
    private[bson4jackson] var state: BsonParser.State.Value = State.FIELDNAME
  }

  /**
   * Extends {@link JsonLocation} to offer a specialized string representation
   */
  private object BsonLocation {
    private final val serialVersionUID: Long = -5441597278886285168L
  }

  private class BsonLocation(
      srcRef: AnyRef, totalBytes: Long
    ) extends JsonLocation(srcRef, totalBytes, -1, -1, -1){

    override def toString: String = {
      val sb = new StringBuilder(80)
      sb.append("[Source: ")
      if (getSourceRef == null) {
        sb.append("UNKNOWN")
      }
      else {
        sb.append(getSourceRef.toString)
      }
      sb.append("; pos: ")
      sb.append(getByteOffset())
      sb.append(']')
      sb.toString
    }
  }

}

/**
 * Constructs a new parser
 * @param jsonFeatures bit flag composed of bits that indicate which
 * { @link org.codehaus.jackson.JsonParser.Feature}s are enabled.
 * @param bsonFeatures bit flag composed of bits that indicate which
 * { @link Feature}s are enabled.
 * @param in the input stream to parse.
 */
class BsonParser(
    jsonFeatures:Int,var _bsonFeatures:Int,var _rawInputStream:InputStream
  ) extends JsonParserMinimalBase(jsonFeatures) {

  import BsonParser._

  locally{
    if (!isEnabled(Feature.HONOR_DOCUMENT_LENGTH)) {

      val in =
        if (!(_rawInputStream.isInstanceOf[BufferedInputStream])) {
          new StaticBufferedInputStream(_rawInputStream)
        }else{
          _rawInputStream
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

  override def getCodec: ObjectCodec = _codec

  override def setCodec(c: ObjectCodec): Unit = {
    _codec = c
  }

  override def close: Unit = {
    if (isEnabled(JsonParser.Feature.AUTO_CLOSE_SOURCE)) {
      _in.close
    }
    _closed = true
  }

  override def nextToken: JsonToken = {
    var ctx = _contexts.peek
    if ((_currToken == null) && (ctx == null)) {
      _currToken = handleNewDocument(false)
    }
    else {
      _tokenPos = _counter.getPosition
      if (ctx == null) {
        if (_currToken == JsonToken.END_OBJECT) {
          return null
        }else{
          throw new JsonParseException("Found element outside the document", getTokenLocation)
        }
      }
      if (ctx.state == State.DONE) {
        ctx.reset
      }
      var readValue = true
      if (ctx.state == State.FIELDNAME) {
        readValue = false

        breakable{
          while (true) {
            ctx.`type` = _in.readByte
            if (ctx.`type` == BsonConstants.TYPE_END) {
              _currToken = (if (ctx.array) JsonToken.END_ARRAY else JsonToken.END_OBJECT)
              _contexts.pop

              break
            }else if (ctx.`type` != BsonConstants.TYPE_UNDEFINED) {
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

              break
            }else{
              skipCString
            }
          }
        }
      }
      if (readValue) {
        ctx.`type` match {
          case BsonConstants.TYPE_DOUBLE =>
            ctx.value = _in.readDouble.asInstanceOf[JDouble]
            _currToken = JsonToken.VALUE_NUMBER_FLOAT
          case BsonConstants.TYPE_STRING =>
            ctx.value = readString
            _currToken = JsonToken.VALUE_STRING
          case BsonConstants.TYPE_DOCUMENT =>
            _currToken = handleNewDocument(false)
          case BsonConstants.TYPE_ARRAY =>
            _currToken = handleNewDocument(true)
          case BsonConstants.TYPE_BINARY =>
            _currToken = handleBinary
          case BsonConstants.TYPE_OBJECTID =>
            ctx.value = readObjectId
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
          case BsonConstants.TYPE_BOOLEAN =>
            val b = _in.readBoolean
            ctx.value = b.asInstanceOf[JBoolean]
            _currToken = (if (b) JsonToken.VALUE_TRUE else JsonToken.VALUE_FALSE)
          case BsonConstants.TYPE_DATETIME =>
            ctx.value = new Date(_in.readLong)
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
          case BsonConstants.TYPE_NULL =>
            _currToken = JsonToken.VALUE_NULL
          case BsonConstants.TYPE_REGEX =>
            _currToken = handleRegEx
          case BsonConstants.TYPE_DBPOINTER =>
            _currToken = handleDBPointer
          case BsonConstants.TYPE_JAVASCRIPT =>
            ctx.value = new JavaScript(readString)
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
          case BsonConstants.TYPE_SYMBOL =>
            ctx.value = readSymbol
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
          case BsonConstants.TYPE_JAVASCRIPT_WITH_SCOPE =>
            _currToken = handleJavascriptWithScope
          case BsonConstants.TYPE_INT32 =>
            ctx.value = _in.readInt.asInstanceOf[JInteger]
            _currToken = JsonToken.VALUE_NUMBER_INT
          case BsonConstants.TYPE_TIMESTAMP =>
            ctx.value = readTimestamp
            _currToken = JsonToken.VALUE_EMBEDDED_OBJECT
          case BsonConstants.TYPE_INT64 =>
            ctx.value = _in.readLong.asInstanceOf[JLong]
            _currToken = JsonToken.VALUE_NUMBER_INT
          case BsonConstants.TYPE_MINKEY =>
            ctx.value = "MinKey"
            _currToken = JsonToken.VALUE_STRING
          case BsonConstants.TYPE_MAXKEY =>
            ctx.value = "MaxKey"
            _currToken = JsonToken.VALUE_STRING
          case _ =>
            throw new JsonParseException("Unknown element type " + ctx.`type`, getTokenLocation)
        }
        ctx.state = State.DONE
      }
    }
     _currToken
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
      val buf = new Array[Byte](JInteger.SIZE / JByte.SIZE)
      var len = 0
      while (len < buf.length) {
        var l = _rawInputStream.read(buf, len, buf.length - len)
        if (l == -1) {
          throw new IOException("Not enough bytes for length of document")
        }
        len += l
      }
      val documentLength = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
      var in: InputStream = new BoundedInputStream(_rawInputStream, documentLength - buf.length)
      if (!(_rawInputStream.isInstanceOf[BufferedInputStream])) {
        in = new StaticBufferedInputStream(in)
      }
      _counter = new CountingInputStream(in)
      _in = new LittleEndianInputStream(_counter)
    }
    else {
      _in.readInt
    }
    _contexts.push(new BsonParser.Context(array))
    (if (array) JsonToken.START_ARRAY else JsonToken.START_OBJECT)
  }

  /**
   * Reads binary data from the input stream
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleBinary: JsonToken = {
    val size = _in.readInt
    val subtype = _in.readByte
    val ctx = getContext
    subtype match {
      case BsonConstants.SUBTYPE_BINARY_OLD =>
        val size2 = _in.readInt
        val buf2  = new Array[Byte](size2)
        _in.readFully(buf2)
        ctx.value = buf2
      case BsonConstants.SUBTYPE_UUID =>
        val l1 = _in.readLong
        val l2 = _in.readLong
        ctx.value = new UUID(l1, l2)
      case _ =>
        val buf = new Array[Byte](size)
        _in.readFully(buf)
        ctx.value = buf
    }
    JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Converts a BSON regex pattern string to a combined value of Java flags that
   * can be used in {@link Pattern#compile(String, int)}
   * @param pattern the regex pattern string
   * @return the Java flags
   * @throws JsonParseException if the pattern string contains a unsupported flag
   */
  protected def regexStrToFlags(pattern: String): Int = {
    var flags = 0
    var i = 0
    while (i < pattern.length) {
      pattern.charAt(i) match {
        case 'i' =>
          flags |= Pattern.CASE_INSENSITIVE
        case 'm' =>
          flags |= Pattern.MULTILINE
        case 's' =>
          flags |= Pattern.DOTALL
        case 'u' =>
          flags |= Pattern.UNICODE_CASE
        case 'l' | 'x' =>
        case _ =>
          throw new JsonParseException("Invalid regex", getTokenLocation)
      }
      i += 1
    }
    flags
  }

  /**
   * Reads and compiles a regular expression
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleRegEx: JsonToken = {
    val regex = readCString
    val pattern = readCString
    getContext.value = Pattern.compile(regex, regexStrToFlags(pattern))
    JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Reads a DBPointer from the stream
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleDBPointer: JsonToken = {
    val pointer = new LinkedHashMap[String,Any]()
    pointer.put("$ns", readString)
    pointer.put("$id", readObjectId)
    getContext.value = pointer
    JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * Can be called when embedded javascript code with scope is found. Reads
   * the code and the embedded document.
   * @return the json token read
   * @throws IOException if an I/O error occurs
   */
  protected def handleJavascriptWithScope: JsonToken = {
    _in.readInt
    val code = readString
    val doc = readDocument
    getContext.value = new JavaScript(code, doc)
    return JsonToken.VALUE_EMBEDDED_OBJECT
  }

  /**
   * @return a null-terminated string read from the input stream
   * @throws IOException if the string could not be read
   */
  protected def readCString: String = _in.readUTF(-1)

  /**
   * Skips over a null-terminated string in the input stream
   * @throws IOException if an I/O error occurs
   */
  protected def skipCString: Unit = {
    while (_in.readByte != 0){}
  }

  /**
   * Reads a string that consists of a integer denoting the number of bytes,
   * the bytes (including a terminating 0 byte)
   * @return the string
   * @throws IOException if the string could not be read
   */
  protected def readString: String = {
    val bytes = _in.readInt
    if (bytes <= 0) {
      throw new IOException("Invalid number of string bytes")
    }
    val s =
      if (bytes > 1)
        _in.readUTF(bytes - 1)
      else
        ""

    _in.readByte
    s
  }

  /**
   * Reads a symbol object from the input stream
   * @return the symbol
   * @throws IOException if the symbol could not be read
   */
  protected def readSymbol: Symbol = new Symbol(readString)

  /**
   * Reads a timestamp object from the input stream
   * @return the timestamp
   * @throws IOException if the timestamp could not be read
   */
  protected def readTimestamp: Timestamp = {
    val inc  = _in.readInt
    val time = _in.readInt
    new Timestamp(time, inc)
  }

  /**
   * Reads a ObjectID from the input stream
   * @return the ObjectID
   * @throws IOException if the ObjectID could not be read
   */
  protected def readObjectId: ObjectId = {
    val Seq(time,machine,inc) = (1 to 3).map{i =>
      ByteOrderUtil.flip(_in.readInt)
    }
    new ObjectId(time, machine, inc)
  }

  /**
   * Fully reads an embedded document, reusing this parser
   * @return the parsed document
   * @throws IOException if the document could not be read
   */
  protected def readDocument: JMap[String,Any] = {
    val codec = getCodec
    if (codec == null) {
      throw new IllegalStateException("Could not parse embedded document because BSON parser has no codec")
    }
    _currToken = handleNewDocument(false)
    codec.readValue(this, new TypeReference[JMap[String,Any]] {})
  }

  /**
   * @return the context of the current element
   * @throws IOException if there is no context
   */
  protected def getContext: BsonParser.Context = {
    Option(_contexts.peek).getOrElse{
      throw new IOException("Context unknown")
    }
  }

  override def isClosed: Boolean = _closed

  override def getCurrentName: String = {
    Option(_contexts.peek).map{_.fieldName}.orNull
  }

  override def getParsingContext: JsonStreamContext = null

  override def getTokenLocation: JsonLocation = new BsonParser.BsonLocation(_in, _tokenPos)

  override def getCurrentLocation: JsonLocation = {
    new BsonParser.BsonLocation(_in, _counter.getPosition)
  }

  override def getText: String = {
    val ctx = _contexts.peek()
    if (ctx == null || ctx.state == State.FIELDNAME) {
      return null
    }
    if (ctx.state == State.VALUE) {
      return ctx.fieldName
    }
    return ctx.value.asInstanceOf[String]
  }

  override def getTextCharacters: Array[Char] = getText.toCharArray

  override def getTextLength: Int = getText.length

  override val getTextOffset = 0

  override val hasTextCharacters = false

  override def getNumberValue: Number = getContext.value.asInstanceOf[Number]

  override def getNumberType: JsonParser.NumberType = {
    import JsonParser.NumberType

    val ctx = _contexts.peek()
    if (ctx == null) {
      null
    }else
    if (ctx.value.isInstanceOf[JInteger]) {
      NumberType.INT
    }
    else if (ctx.value.isInstanceOf[JLong]) {
      NumberType.LONG
    }
    else if (ctx.value.isInstanceOf[BigInteger]) {
      NumberType.BIG_INTEGER
    }
    else if (ctx.value.isInstanceOf[JFloat]) {
      NumberType.FLOAT
    }
    else if (ctx.value.isInstanceOf[JDouble]) {
      NumberType.DOUBLE
    }
    else if (ctx.value.isInstanceOf[BigDecimal]) {
      NumberType.BIG_DECIMAL
    }else{
      null // TODO ?
    }
  }

  override def getIntValue: Int = {
    return (getContext.value.asInstanceOf[Number]).intValue
  }

  override def getLongValue: Long = {
    return (getContext.value.asInstanceOf[Number]).longValue
  }

  override def getBigIntegerValue: BigInteger = {
    val n = getNumberValue
    if (n == null) {
      null
    }else if (n.isInstanceOf[JByte] || n.isInstanceOf[JInteger] || n.isInstanceOf[JLong] || n.isInstanceOf[JShort]) {
      BigInteger.valueOf(n.longValue)
    }else if (n.isInstanceOf[JDouble] || n.isInstanceOf[JFloat]) {
      BigDecimal.valueOf(n.doubleValue).toBigInteger
    }else{
      new BigInteger(n.toString)
    }
  }

  override def getFloatValue: Float = {
    (getContext.value.asInstanceOf[Number]).floatValue
  }

  override def getDoubleValue: Double = {
    (getContext.value.asInstanceOf[Number]).doubleValue
  }

  override def getDecimalValue: BigDecimal = {
    val n = getNumberValue()
    if (n == null) {
      null
    }else if (n.isInstanceOf[JByte] || n.isInstanceOf[JInteger] || n.isInstanceOf[JLong] || n.isInstanceOf[JShort]) {
      BigDecimal.valueOf(n.longValue)
    }
    else if (n.isInstanceOf[JDouble] || n.isInstanceOf[JFloat]) {
      BigDecimal.valueOf(n.doubleValue)
    }else{
      new BigDecimal(n.toString)
    }
  }

  override def getBinaryValue(b64variant: Base64Variant): Array[Byte] = getText.getBytes

  override def getEmbeddedObject: AnyRef = {
    val ctx = _contexts.peek
    (if (ctx != null) ctx.value else null)
  }

  override protected def _handleEOF: Unit = {
    _reportInvalidEOF
  }

  /**
   * The input stream to read from
   */
  private var _in: LittleEndianInputStream = null
  /**
   * Counts the number of bytes read from {@link #_in}
   */
  private var _counter: CountingInputStream = null
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
  private val _contexts:Deque[Context] = new ArrayDeque[Context]()
}
