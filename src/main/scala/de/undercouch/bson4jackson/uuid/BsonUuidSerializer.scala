// Copyright 2010-2011 Ed Anuff
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

package de.undercouch.bson4jackson.uuid

import java.io.IOException
import java.util.UUID

import org.codehaus.jackson.JsonGenerationException
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonProcessingException
import org.codehaus.jackson.map.JsonSerializer
import org.codehaus.jackson.map.SerializerProvider

import de.undercouch.bson4jackson.BsonConstants
import de.undercouch.bson4jackson.BsonGenerator

/**
 * Serializer for writing UUIDs as BSON binary fields with UUID subtype. Can
 * only be used in conjunction with the BsonGenerator.
 * @author Ed Anuff
 */
class BsonUuidSerializer extends JsonSerializer[UUID]{

  @throws(classOf[IOException])
  @throws(classOf[JsonProcessingException])
  override def serialize( value:UUID,jgen:JsonGenerator,provider:SerializerProvider){
    if (!(jgen.isInstanceOf[BsonGenerator])) {
      throw new JsonGenerationException("BsonUuidSerializer can " +
          "only be used with BsonGenerator")
    }
    jgen.asInstanceOf[BsonGenerator].writeBinary(null, BsonConstants.SUBTYPE_UUID,
        BsonUuidSerializer.uuidToLittleEndianBytes(value), 0, 16)
  }
}

object BsonUuidSerializer{
  /**
   * Utility routine for converting UUIDs to bytes in little endian format.
   * @param uuid The UUID to convert
   * @return a byte array representing the UUID in little endian format
   */
  private def uuidToLittleEndianBytes(uuid:UUID):Array[Byte] = {
    val msb = uuid.getMostSignificantBits()
    val lsb = uuid.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    var i = 0
    while(i < 8) {
      buffer(i) = (msb >>> 8 * i).asInstanceOf[Byte]
      i += 1
    }
    while(i < 16) {
      buffer(i) = (lsb >>> 8 * (i - 16)).asInstanceOf[Byte]
      i += 1
    }

    buffer
  }
}
