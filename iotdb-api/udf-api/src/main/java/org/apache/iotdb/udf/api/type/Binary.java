/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.udf.api.type;

import org.apache.tsfile.utils.Pair;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Override compareTo() and equals() function to Binary class. This class is used to accept Java
 * String type
 */
public class Binary implements Comparable<Binary>, Serializable {

  private static final long serialVersionUID = 1250049718612917815L;
  public static final String STRING_ENCODING = "UTF-8";
  public static final Charset STRING_CHARSET = StandardCharsets.UTF_8;

  private final byte[] values;

  private int hash;

  private int length;

  // indicate whether hash has been calculated
  private boolean hasCalculatedHash;

  private String stringCache;

  /** if the bytes v is modified, the modification is visible to this binary. */
  public Binary(byte[] v) {
    this.values = v;
    this.length = (v == null) ? -1 : v.length;
  }

  public Binary(String s) {
    this.values = (s == null) ? null : s.getBytes(STRING_CHARSET);
    this.length = (s == null) ? -1 : values.length;
  }

  public Binary(byte[] v, int length) {
    this.values = v;
    this.length = length;
  }

  public static Binary valueOf(String value) {
    return new Binary(stringToBytes(value));
  }

  @Override
  public int compareTo(Binary other) {
    if (other == null) {
      if (this.values == null) {
        return 0;
      } else {
        return 1;
      }
    }

    int i = 0;
    while (i < getLength() && i < other.getLength()) {
      if (this.values[i] == other.values[i]) {
        i++;
        continue;
      }
      return this.values[i] - other.values[i];
    }
    return getLength() - other.getLength();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    return compareTo((Binary) other) == 0;
  }

  @Override
  public int hashCode() {
    if (!hasCalculatedHash) {
      // copied from Arrays.hashCode
      hash = 0;
      if (values != null) {
        hash = 1;
        for (int i = 0; i < length; i++) {
          hash = 31 * hash + values[i];
        }
      }
      hasCalculatedHash = true;
    }
    return hash;
  }

  /**
   * Get length of values. Returns -1 if values is null.
   *
   * @return length
   */
  public int getLength() {
    return length;
  }

  public boolean isNull() {
    return values == null;
  }

  public String getStringValue() {
    if (values == null) {
      return null;
    }
    if (stringCache == null) {
      stringCache = new String(this.values, 0, length, STRING_CHARSET);
    }
    return stringCache;
  }

  public String getTextEncodingType() {
    return STRING_ENCODING;
  }

  @Override
  public String toString() {
    return getStringValue();
  }

  public Pair<byte[], Integer> getValuesAndLength() {
    return new Pair<>(values, length);
  }

  /**
   * convert string to byte array using UTF-8 encoding.
   *
   * @param str input string
   * @return byte array
   */
  public static byte[] stringToBytes(String str) {
    return str.getBytes(STRING_CHARSET);
  }
}
