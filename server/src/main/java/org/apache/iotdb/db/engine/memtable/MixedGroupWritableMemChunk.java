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

package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.MixedGroupChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public class MixedGroupWritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;

  private TVList list;

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  public MixedGroupWritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newMixedGroupTVList(schema.getType());
  }

  @Override
  public void putLong(long t, long v) {}

  @Override
  public void putInt(long t, int v) {}

  @Override
  public void putFloat(long t, float v) {}

  @Override
  public void putDouble(long t, double v) {}

  @Override
  public void putBinary(long t, Binary v) {}

  @Override
  public void putBoolean(long t, boolean v) {}

  @Override
  public void putAlignedValue(long t, Object[] v, int[] columnIndexArray) {}

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {}

  @Override
  public void putAlignedValues(
      long[] t, Object[] v, BitMap[] bitMaps, int[] columnIndexArray, int start, int end) {}

  @Override
  public void write(long insertTime, Object objectValue) {}

  @Override
  public void writeAlignedValue(
      long insertTime,
      Object[] objectValue,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList) {}

  @Override
  public void write(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {}

  @Override
  public void writeAlignedValues(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {}

  @Override
  public long count() {
    return list.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public TVList getSortedTvListForQuery(List<IMeasurementSchema> schemaList) {
    return null;
  }

  @Override
  public void sortTvListForFlush() {
    sortTVList();
  }

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return 0;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new MixedGroupChunkWriterImpl(schema);
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    MixedGroupChunkWriterImpl chunkWriterImpl = (MixedGroupChunkWriterImpl) chunkWriter;

    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);
      int deviceId = list.getDeviceIdentifier(sortedRowIndex);
      switch (schema.getType()) {
        case BOOLEAN:
          chunkWriterImpl.write(time, list.getBoolean(sortedRowIndex), deviceId);
          break;
        case INT32:
          chunkWriterImpl.write(time, list.getInt(sortedRowIndex), deviceId);
          break;
        case INT64:
          chunkWriterImpl.write(time, list.getLong(sortedRowIndex), deviceId);
          break;
        case FLOAT:
          chunkWriterImpl.write(time, list.getFloat(sortedRowIndex), deviceId);
          break;
        case DOUBLE:
          chunkWriterImpl.write(time, list.getDouble(sortedRowIndex), deviceId);
          break;
        case TEXT:
          //                    chunkWriterImpl.write(time, list.getBinary(sortedRowIndex));
          break;
        default:
          // LOGGER.error("WritableMemChunk does not support data type: {}", schema.getType());
          break;
      }
    }
  }

  @Override
  public void release() {
    if (list.getReferenceCount() == 0) {
      list.clear();
    }
  }

  @Override
  public long getFirstPoint() {
    return 0;
  }

  @Override
  public long getLastPoint() {
    return 0;
  }

  @Override
  public void write(long insertTime, Object value, byte deviceId) {
    switch (schema.getType()) {
      case BOOLEAN:
        list.putBoolean(insertTime, (boolean) value, deviceId);
        break;
      case INT32:
        list.putInt(insertTime, (int) value, deviceId);
        break;
      case INT64:
        list.putLong(insertTime, (long) value, deviceId);
        break;
      case FLOAT:
        list.putFloat(insertTime, (float) value, deviceId);
        break;
      case DOUBLE:
        list.putDouble(insertTime, (double) value, deviceId);
        break;
      case TEXT:
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
    }
  }

  @Override
  public TVList getTVList() {
    return list;
  }
}
