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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class BinaryTVList extends TVList {

  // list of primitive array, add 1 when expanded -> Binary primitive array
  // index relation: arrayIndex -> elementIndex
  private List<Binary[]> values;

  private Binary[][] sortedValues;

  private Binary pivotValue;

  BinaryTVList() {
    super();
    values = new ArrayList<>();
  }

  @Override
  public void putBinary(long timestamp, Binary value) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    rowCount++;
    if (sorted && rowCount > 1 && timestamp < getTime(rowCount - 2)) {
      sorted = false;
    }
  }

  @Override
  public Binary getBinary(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  protected void set(int index, long timestamp, Binary value) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public BinaryTVList clone() {
    BinaryTVList cloneList = new BinaryTVList();
    cloneAs(cloneList);
    for (Binary[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private Binary[] cloneValue(Binary[] array) {
    Binary[] cloneArray = new Binary[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null
        || sortedTimestamps.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
    }
    if (sortedValues == null
        || sortedValues.length < PrimitiveArrayManager.getArrayRowCount(rowCount)) {
      sortedValues =
          (Binary[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.TEXT, rowCount);
    }
    sort(0, rowCount);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (Binary[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  void clearSortedValue() {
    if (sortedValues != null) {
      sortedValues = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    Binary srcV = getBinary(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedValues[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getBinary(src);
  }

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      Binary loV = getBinary(lo);
      long hiT = getTime(hi);
      Binary hiV = getBinary(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() {
    values.add((Binary[]) getPrimitiveArraysByType(TSDataType.TEXT));
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getBinary(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), TsPrimitiveType.getByType(TSDataType.TEXT, getBinary(index)));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.TEXT, getBinary(index)));
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(values.remove(values.size() - 1));
  }

  @Override
  public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    checkExpansion();

    int idx = start;
    // constraint: time.length + timeIdxOffset == value.length
    int timeIdxOffset = 0;
    if (bitMap != null && !bitMap.isAllUnmarked()) {
      // time array is a reference, should clone necessary time values
      long[] clonedTime = new long[end - start];
      System.arraycopy(time, start, clonedTime, 0, end - start);
      time = clonedTime;
      timeIdxOffset = start;
      // drop null at the end of value array
      int nullCnt =
          dropNullValThenUpdateMinTimeAndSorted(time, value, bitMap, start, end, timeIdxOffset);
      end -= nullCnt;
    } else {
      updateMinTimeAndSorted(time, start, end);
    }

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = rowCount / ARRAY_SIZE;
      int elementIdx = rowCount % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
        rowCount += inputRemaining;
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
        idx += internalRemaining;
        rowCount += internalRemaining;
        checkExpansion();
      }
    }
  }

  // move null values to the end of time array and value array, then return number of null values
  int dropNullValThenUpdateMinTimeAndSorted(
      long[] time, Binary[] values, BitMap bitMap, int start, int end, int tIdxOffset) {
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;

    int nullCnt = 0;
    for (int vIdx = start; vIdx < end; vIdx++) {
      if (bitMap.isMarked(vIdx)) {
        nullCnt++;
        continue;
      }
      // move value ahead to replace null
      int tIdx = vIdx - tIdxOffset;
      if (nullCnt != 0) {
        time[tIdx - nullCnt] = time[tIdx];
        values[vIdx - nullCnt] = values[vIdx];
      }
      // update minTime and sorted
      tIdx = tIdx - nullCnt;
      inPutMinTime = Math.min(inPutMinTime, time[tIdx]);
      if (inputSorted && tIdx > 0 && time[tIdx - 1] > time[tIdx]) {
        inputSorted = false;
      }
    }
    minTime = Math.min(inPutMinTime, minTime);
    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
    return nullCnt;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  public TVList splitByFlushingWindow(double flushingWindowProportion) {
    BinaryTVList flushTVList = new BinaryTVList();
    int flushingCount = (int) (flushingWindowProportion * rowCount);

    int arrayIndex = flushingCount / ARRAY_SIZE;
    int elementIndex = flushingCount % ARRAY_SIZE;

    flushTVList.rowCount = flushingCount;

    for (int i = 0; i < arrayIndex; i++) {
      flushTVList.timestamps.add(timestamps.get(i));
      flushTVList.values.add(values.get(i));
    }
    long[] timestampFlushArray = (long[]) getPrimitiveArraysByType(TSDataType.INT64);
    Binary[] valueFlushArray = (Binary[]) getPrimitiveArraysByType(TSDataType.INT64);
    System.arraycopy(timestamps.get(arrayIndex), 0, timestampFlushArray, 0, elementIndex);
    System.arraycopy(values.get(arrayIndex), 0, valueFlushArray, 0, elementIndex);
    flushTVList.timestamps.add(timestampFlushArray);
    flushTVList.values.add(valueFlushArray);

    List<long[]> timestampWindow = timestamps;
    List<Binary[]> valueWindow = values;

    timestamps = new ArrayList<>();
    values = new ArrayList<>();
    int newRowCount = rowCount - flushingCount;

    long[] remainTime = new long[newRowCount];
    Binary[] remainValue = new Binary[newRowCount];
    int existCount = 0;
    for (; arrayIndex <= timestampWindow.size(); arrayIndex++) {
      if (newRowCount - existCount >= ARRAY_SIZE) {
        System.arraycopy(
            timestampWindow.get(arrayIndex),
            elementIndex,
            remainTime,
            existCount,
            ARRAY_SIZE - elementIndex);
        System.arraycopy(
            valueWindow.get(arrayIndex),
            elementIndex,
            remainValue,
            existCount,
            ARRAY_SIZE - elementIndex);
        existCount += (ARRAY_SIZE - existCount);
        elementIndex = 0;
      } else {
        System.arraycopy(
            timestampWindow.get(arrayIndex),
            elementIndex,
            remainTime,
            existCount,
            rowCount - existCount);
        System.arraycopy(
            valueWindow.get(arrayIndex),
            elementIndex,
            remainValue,
            existCount,
            rowCount - existCount);
      }
      PrimitiveArrayManager.release(timestampWindow.get(arrayIndex));
      PrimitiveArrayManager.release(valueWindow.get(arrayIndex));
    }

    rowCount = 0;
    putBinaries(remainTime, remainValue, null, 0, newRowCount);

    return flushTVList;
  }
}
