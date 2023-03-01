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

package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.MixedGroupBatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MixedGroupPageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /** decoder for value column */
  protected Decoder valueDecoder;

  /** decoder for time column */
  protected Decoder timeDecoder;

  protected Decoder deviceColumnDecoder;

  /** time column in memory */
  protected ByteBuffer timeBuffer;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  protected ByteBuffer deviceColumnBuffer;

  protected Filter filter;

  private int deviceIdentifier;

  private boolean getAllData;

  public MixedGroupPageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Decoder deviceColumnDecoder,
      Filter filter,
      int deviceIdentifier,
      boolean getAllData) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.deviceColumnDecoder = deviceColumnDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
    this.deviceIdentifier = deviceIdentifier;
    this.getAllData = getAllData;
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    if (getAllData) {
      return createMixedGroupBatchData();
    }

    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    if (filter == null || filter.satisfy(getStatistics())) {
      while (timeDecoder.hasNext(timeBuffer)) {
        long timestamp = timeDecoder.readLong(timeBuffer);
        int deviceId = deviceColumnDecoder.readInt(deviceColumnBuffer);

        // todo
        if (deviceId > deviceIdentifier) {
          break;
        }

        switch (dataType) {
          case BOOLEAN:
            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, aBoolean))) {
              pageData.putBoolean(timestamp, aBoolean);
            }
            break;
          case INT32:
            int anInt = valueDecoder.readInt(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, anInt))) {
              pageData.putInt(timestamp, anInt);
            }
            break;
          case INT64:
            long aLong = valueDecoder.readLong(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, aLong))) {
              pageData.putLong(timestamp, aLong);
            }
            break;
          case FLOAT:
            float aFloat = valueDecoder.readFloat(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, aFloat))) {
              pageData.putFloat(timestamp, aFloat);
            }
            break;
          case DOUBLE:
            double aDouble = valueDecoder.readDouble(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, aDouble))) {
              pageData.putDouble(timestamp, aDouble);
            }
            break;
          case TEXT:
            Binary aBinary = valueDecoder.readBinary(valueBuffer);
            if (deviceId == deviceIdentifier
                && (filter == null || filter.satisfy(timestamp, aBinary))) {
              pageData.putBinary(timestamp, aBinary);
            }
            break;
          default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
      }
    }
    return pageData.flip();
  }

  private BatchData createMixedGroupBatchData() throws IOException {
    MixedGroupBatchData pageData = new MixedGroupBatchData(dataType);

    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      int deviceId = deviceColumnDecoder.readInt(deviceColumnBuffer);
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          pageData.putBoolean(timestamp, aBoolean, deviceId);
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          pageData.putInt(timestamp, anInt, deviceId);
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          pageData.putLong(timestamp, aLong, deviceId);
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          pageData.putFloat(timestamp, aFloat, deviceId);
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          pageData.putDouble(timestamp, aDouble, deviceId);
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          pageData.putBinary(timestamp, aBinary, deviceId);
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }

    return pageData.flip();
  }

  @Override
  public Statistics getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);
    int valueBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
    valueBuffer.limit(timeBufferLength + valueBufferLength);

    deviceColumnBuffer = pageData.slice();
    deviceColumnBuffer.position(timeBufferLength + valueBufferLength);
  }
}
