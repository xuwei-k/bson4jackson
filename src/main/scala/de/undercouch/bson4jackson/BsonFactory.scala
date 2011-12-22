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

package de.undercouch.bson4jackson

import java.io.{
  ByteArrayInputStream,IOException,InputStream
 ,OutputStream,Reader,Writer
}

import org.codehaus.jackson.{
  JsonEncoding,JsonFactory,JsonGenerator
 ,JsonParseException,JsonParser,ObjectCodec
}
import org.codehaus.jackson.io.IOContext

object BsonFactory{
  /**
   * The BSON generator features enabled by default
   */
  private val DEFAULT_BSON_GENERATOR_FEATURE_FLAGS = 0

  /**
   * The BSON parser features enabled by default
   */
  private val DEFAULT_BSON_PARSER_FEATURE_FLAGS = 0
}

/**
 * Factory for {@link BsonGenerator} and {@link BsonParser}
 * @author Michel Kraemer
 */
class BsonFactory protected[bson4jackson](oc:ObjectCodec) extends JsonFactory(oc) {
  import BsonFactory._

  /**
   * The BSON generator features to be enabled when a new
   * generator is created
   */
  protected var _bsonGeneratorFeatures = DEFAULT_BSON_GENERATOR_FEATURE_FLAGS

  /**
   * The BSON parser features to be enabled when a new parser
   * is created
   */
  protected var _bsonParserFeatures = DEFAULT_BSON_PARSER_FEATURE_FLAGS

  /**
   * @see JsonFactory#JsonFactory()
   */
  def this() {
    this(null)
  }

  /**
   * Method for enabling/disabling specified generator features
   * (check {@link BsonGenerator.Feature} for list of features)
   * @param f the feature to enable or disable
   * @param state true if the feature should be enabled, false otherwise
   */
  final def configure(f:BsonGenerator.Feature,state:Boolean):BsonFactory = {
    if (state)
      enable(f)
    else
      disable(f)
  }

  /**
   * Method for enabling specified generator features
   * (check {@link BsonGenerator.Feature} for list of features)
   * @param f the feature to enable
   */
  def enable(f:BsonGenerator.Feature):BsonFactory = {
    _bsonGeneratorFeatures |= f.getMask
    this
  }

  /**
   * Method for disabling specified generator features
   * (check {@link BsonGenerator.Feature} for list of features)
   * @param f the feature to disable
   */

  def disable(f:BsonGenerator.Feature):BsonFactory = {
    _bsonGeneratorFeatures &= ~f.getMask
    this
  }

  /**
   * @return true if the specified generator feature is enabled
   */
  final def isEnabled(f:BsonGenerator.Feature):Boolean = {
    (_bsonGeneratorFeatures & f.getMask) != 0
  }

  /**
   * Method for enabling/disabling specified parser features
   * (check {@link BsonParser.Feature} for list of features)
   * @param f the feature to enable or disable
   * @param state true if the feature should be enabled, false otherwise
   */
  final def configure(f:BsonParser.Feature,state:Boolean):BsonFactory = {
    if (state)
      enable(f)
    else
      disable(f)
  }

  /**
   * Method for enabling specified parser features
   * (check {@link BsonParser.Feature} for list of features)
   * @param f the feature to enable
   */
  def enable(f:BsonParser.Feature):BsonFactory = {
    _bsonParserFeatures |= f.getMask
    this
  }

  /**
   * Method for disabling specified parser features
   * (check {@link BsonParser.Feature} for list of features)
   * @param f the feature to disable
   */
  def disable(f:BsonParser.Feature):BsonFactory = {
    _bsonParserFeatures &= ~f.getMask
    this
  }

  /**
   * @return true if the specified parser feature is enabled
   */
  final def isEnabled(f:BsonParser.Feature):Boolean = {
    (_bsonParserFeatures & f.getMask) != 0
  }

  @throws(classOf[IOException])
  override def createJsonGenerator(out:OutputStream, enc:JsonEncoding):BsonGenerator = {
    createJsonGenerator(out)
  }

  @throws(classOf[IOException])
  def createJsonGenerator(out:OutputStream):BsonGenerator = {
    val g = new BsonGenerator(_generatorFeatures, _bsonGeneratorFeatures, out)
    Option(getCodec()).foreach{
      g.setCodec
    }
    g
  }

  @throws(classOf[IOException])
  override def createJsonParser(in:InputStream):BsonParser = {
    val p = new BsonParser(_parserFeatures, _bsonParserFeatures, in)
    Option(getCodec()).foreach{
      p.setCodec
    }
    p
  }

  @throws(classOf[IOException])
  @throws(classOf[JsonParseException])
  override protected def _createJsonParser(in:InputStream, ctxt:IOContext):JsonParser = {
    createJsonParser(in)
  }

  @throws(classOf[IOException])
  @throws(classOf[JsonParseException])
  override protected def _createJsonParser(r:Reader,ctxt:IOContext):JsonParser = {
    throw new UnsupportedOperationException("Can not create reader for non-byte-based source")
  }

  @throws(classOf[IOException])
  @throws(classOf[JsonParseException])
  override protected def _createJsonParser(data:Array[Byte],offset:Int,len:Int,ctxt:IOContext):JsonParser = {
    _createJsonParser(new ByteArrayInputStream(data, offset, len), ctxt)
  }

  @throws(classOf[IOException])
  override protected def _createUTF8JsonGenerator(out:OutputStream, ctxt:IOContext):JsonGenerator = {
    createJsonGenerator(out, ctxt.getEncoding())
  }

  @throws(classOf[IOException])
  override protected def _createJsonGenerator(out:Writer, ctxt:IOContext):JsonGenerator = {
    throw new UnsupportedOperationException("Can not create generator for non-byte-based target")
  }

  @throws(classOf[IOException])
  override protected def _createWriter(out:OutputStream, enc:JsonEncoding, ctxt:IOContext):Writer = {
    throw new UnsupportedOperationException("Can not create generator for non-byte-based target")
  }
}
