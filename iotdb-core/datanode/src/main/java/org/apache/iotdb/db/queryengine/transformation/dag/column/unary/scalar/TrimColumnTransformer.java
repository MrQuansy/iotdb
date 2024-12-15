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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

public class TrimColumnTransformer extends UnaryColumnTransformer {

  private final byte[] character;

  public TrimColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, String characterStr) {
    super(returnType, childColumnTransformer);
    this.character = characterStr.getBytes();
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        Pair<byte[], Integer> result = column.getBinary(i).getValuesAndLength();
        columnBuilder.writeBinary(
            new Binary(trim(result.left, 0, result.right, character, 0, character.length)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        Pair<byte[], Integer> currentValue = column.getBinary(i).getValuesAndLength();
        columnBuilder.writeBinary(
            new Binary(
                trim(currentValue.left, 0, currentValue.right, character, 0, character.length)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  public static byte[] trim(
      byte[] source, int aOffset, int aLength, byte[] character, int bOffset, int bLength) {
    if (aLength == 0 || bLength == 0) {
      return source;
    }
    int start = aOffset;
    int end = start + aLength - 1;

    while (start <= end && isContain(character, bOffset, bLength, source[start])) {
      start++;
    }
    while (start <= end && isContain(character, bOffset, bLength, source[end])) {
      end--;
    }
    if (start > end) {
      return new byte[0];
    } else {
      byte[] result = new byte[end - start + 1];
      System.arraycopy(source, start, result, 0, end - start + 1);
      return result;
    }
  }

  public static byte[] trim(byte[] source, byte[] character) {
    return trim(source, 0, source.length, character, 0, character.length);
  }

  public static boolean isContain(byte[] character, int offset, int length, byte target) {
    for (int i = offset; i < offset + length; i++) {
      if (character[i] == target) {
        return true;
      }
    }
    return false;
  }

  public static boolean isContain(byte[] character, byte target) {
    for (byte b : character) {
      if (b == target) {
        return true;
      }
    }
    return false;
  }
}
