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
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class MixedGroupIntTVList extends IntTVList {

  private List<byte[]> deviceIdentifier;
  private byte[][] sortedDeviceIdentifier;
  private byte pivotIdentifier;

  public MixedGroupIntTVList() {
    deviceIdentifier = new ArrayList<>();
  }

  @Override
  public void putInt(long time, int value, byte deviceIdentifier) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    minTime = Math.min(minTime, time);
    timestamps.get(arrayIndex)[elementIndex] = time;
    values.get(arrayIndex)[elementIndex] = value;
    this.deviceIdentifier.get(arrayIndex)[elementIndex] = deviceIdentifier;
    rowCount++;
    if ((rowCount > 1 && sorted)
        && (deviceIdentifier < getDeviceIdentifier(rowCount - 2)
            || (deviceIdentifier == getDeviceIdentifier(rowCount - 2)
                && time < getTime(rowCount - 2)))) {
      sorted = false;
    }
  }

  @Override
  public byte getDeviceIdentifier(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return deviceIdentifier.get(arrayIndex)[elementIndex];
  }

  @Override
  protected void checkExpansion() {
    if ((rowCount % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
      deviceIdentifier.add((byte[]) getPrimitiveArraysByType(TSDataType.BYTE));
    }
  }

  @Override
  public IntTVList clone() {
    MixedGroupIntTVList cloneList = new MixedGroupIntTVList();
    cloneAs(cloneList);
    for (byte[] deviceIdentifierArray : deviceIdentifier) {
      cloneList.deviceIdentifier.add(cloneDevice(deviceIdentifierArray));
    }
    for (int[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null
        || sortedTimestamps.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
      sortedDeviceIdentifier =
          (byte[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.BYTE, rowCount);
    }
    if (sortedValues == null
        || sortedValues.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedValues =
          (int[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT32, rowCount);
    }
    sort(0, rowCount);
    clearSortedValue();
    clearSortedTime();
    sortedDeviceIdentifier = null;
    sorted = true;
  }

  protected byte[] cloneDevice(byte[] array) {
    byte[] cloneArray = new byte[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedValues[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getInt(src);
    sortedDeviceIdentifier[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getDeviceIdentifier(src);
  }

  @Override
  protected int compare(int idx1, int idx2) {
    byte di1 = getDeviceIdentifier(idx1);
    byte di2 = getDeviceIdentifier(idx2);
    int comp = Byte.compare(di1, di2);
    if (comp == 0) {
      long t1 = getTime(idx1);
      long t2 = getTime(idx2);
      return Long.compare(t1, t2);
    }
    return comp;
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    if (dest >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(dest);
    }
    int arrayIndex = dest / ARRAY_SIZE;
    int elementIndex = dest % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE];
    values.get(arrayIndex)[elementIndex] = sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE];
    deviceIdentifier.get(arrayIndex)[elementIndex] =
        sortedDeviceIdentifier[src / ARRAY_SIZE][src % ARRAY_SIZE];
  }

  @Override
  protected void setPivotTo(int dest) {
    if (dest >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(dest);
    }
    int arrayIndex = dest / ARRAY_SIZE;
    int elementIndex = dest % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = pivotTime;
    values.get(arrayIndex)[elementIndex] = pivotValue;
    deviceIdentifier.get(arrayIndex)[elementIndex] = pivotIdentifier;
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getInt(pos);
    pivotIdentifier = getDeviceIdentifier(pos);
  }

  @Override
  public IPointReader getIterator(
      int floatPrecision,
      TSEncoding encoding,
      int size,
      List<TimeRange> deletionList,
      byte deviceIdentifier) {
    return new MixedGroupIte(deviceIdentifier, floatPrecision, encoding, size, deletionList);
  }

  // todo
  @Override
  protected int countRunAndMakeAscending(int lo, int hi) {
    return 0;
  }

  @Override
  protected void set(int src, int dest) {
    if (dest >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(dest);
    }
    int arrayIndex = dest / ARRAY_SIZE;
    int elementIndex = dest % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = getTime(src);
    ;
    values.get(arrayIndex)[elementIndex] = getInt(src);
    deviceIdentifier.get(arrayIndex)[elementIndex] = getDeviceIdentifier(src);
  }

  private class MixedGroupIte extends Ite {

    byte deviceIdentifierForQuery;

    public MixedGroupIte(byte deviceIdentifierForQuery) {
      super();
      this.deviceIdentifierForQuery = deviceIdentifierForQuery;
    }

    public MixedGroupIte(
        byte deviceIdentifierForQuery,
        int floatPrecision,
        TSEncoding encoding,
        int size,
        List<TimeRange> deletionList) {
      super(floatPrecision, encoding, size, deletionList);
      this.deviceIdentifierForQuery = deviceIdentifierForQuery;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      if (hasCachedPair) {
        return true;
      }

      while (cur < iteSize) {
        long time = getTime(cur);
        byte deviceIdentifier = getDeviceIdentifier(cur);
        if ((deviceIdentifier != deviceIdentifierForQuery)
            || isPointDeleted(time)
            || ((cur + 1 < rowCount())
                && (time == getTime(cur + 1))
                && (deviceIdentifierForQuery == getDeviceIdentifier(cur + 1)))) {
          cur++;
          continue;
        }
        TimeValuePair tvPair;
        tvPair = getTimeValuePair(cur, time, floatPrecision, encoding);
        cur++;
        if (tvPair.getValue() != null) {
          cachedTimeValuePair = tvPair;
          hasCachedPair = true;
          return true;
        }
      }

      return false;
    }
  }
}
