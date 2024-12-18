/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.plan.statement.literal;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PooledBinary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.utils.BlobUtils.parseBlobString;

public class BinaryLiteral extends Literal {

  private final byte[] values;
  private final int length;

  public BinaryLiteral(String value) {
    try {
      this.values = parseBlobString(value);
      this.length = values.length;
    } catch (IllegalArgumentException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  public BinaryLiteral(byte[] values) {
    this.values = values;
    this.length = values.length;
  }

  public BinaryLiteral(byte[] values, int length) {
    this.values = values;
    this.length = length;
  }

  public Pair<byte[], Integer> getValuesAndLength() {
    return new Pair<>(values, length);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(LiteralType.BINARY.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(getBinary(), byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(LiteralType.BINARY.ordinal(), stream);
    ReadWriteIOUtils.write(getBinary(), stream);
  }

  @Override
  public Binary getBinary() {
    if (length != values.length) {
      return new PooledBinary(values, length);
    } else {
      return new Binary(values);
    }
  }

  @Override
  public boolean isDataTypeConsistency(TSDataType dataType) {
    return dataType == TSDataType.BLOB;
  }

  @Override
  public String getDataTypeString() {
    return TSDataType.BLOB.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryLiteral that = (BinaryLiteral) o;
    return BytesUtils.byteArrayEquals(values, length, that.values, that.length);
  }

  @Override
  public int hashCode() {
    return BytesUtils.byteArrayHashCode(values, length);
  }
}
