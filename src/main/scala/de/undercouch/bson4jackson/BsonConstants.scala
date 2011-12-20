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

/**
 * Constants used within the BSON format
 * @author Michel Kraemer
 */
object BsonConstants {
  /**
   * End of document
   */
  @inline final val TYPE_END:Byte = 0x00

  /**
   * End of string
   */
  @inline final val END_OF_STRING:Byte = 0x00

  /**
   * Type markers
   */
  @inline final val TYPE_DOUBLE:Byte = 0x01
  @inline final val TYPE_STRING:Byte = 0x02
  @inline final val TYPE_DOCUMENT:Byte = 0x03
  @inline final val TYPE_ARRAY:Byte = 0x04
  @inline final val TYPE_BINARY:Byte = 0x05
  @inline final val TYPE_UNDEFINED:Byte = 0x06
  @inline final val TYPE_OBJECTID:Byte = 0x07
  @inline final val TYPE_BOOLEAN:Byte = 0x08
  @inline final val TYPE_DATETIME:Byte = 0x09
  @inline final val TYPE_NULL:Byte = 0x0A
  @inline final val TYPE_REGEX:Byte = 0x0B
  @inline final val TYPE_DBPOINTER:Byte = 0x0C
  @inline final val TYPE_JAVASCRIPT:Byte = 0x0D
  @inline final val TYPE_SYMBOL:Byte = 0x0E
  @inline final val TYPE_JAVASCRIPT_WITH_SCOPE:Byte = 0x0F
  @inline final val TYPE_INT32:Byte = 0x10
  @inline final val TYPE_TIMESTAMP:Byte = 0x11
  @inline final val TYPE_INT64:Byte = 0x12
  @inline final val TYPE_MINKEY:Byte = 0xFF.asInstanceOf[Byte]
  @inline final val TYPE_MAXKEY:Byte = 0x7f

  /**
   * Binary subtypes
   */
  @inline final val SUBTYPE_BINARY:Byte = 0x00
  @inline final val SUBTYPE_FUNCTION:Byte = 0x01
  @inline final val SUBTYPE_BINARY_OLD:Byte = 0x02
  @inline final val SUBTYPE_UUID:Byte = 0x03
  @inline final val SUBTYPE_MD5:Byte = 0x05
  @inline final val SUBTYPE_USER_DEFINED:Byte = 0x80.asInstanceOf[Byte]
}
