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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public class MixedGroupBatchData extends BatchData {

  protected List<int[]> deviceIdentifierRet;

  public MixedGroupBatchData(TSDataType type) {
    super(type);
    deviceIdentifierRet = new ArrayList<>();
    deviceIdentifierRet.add(new int[capacity]);
  }

  /**
   * put boolean data.
   *
   * @param t timestamp
   * @param v boolean data
   */
  public void putBoolean(long t, boolean v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        booleanRet.add(new boolean[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        boolean[] newValueData = new boolean[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(booleanRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        booleanRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    booleanRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put int data.
   *
   * @param t timestamp
   * @param v int data
   */
  public void putInt(long t, int v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        intRet.add(new int[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        int[] newValueData = new int[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(intRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        intRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    intRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put long data.
   *
   * @param t timestamp
   * @param v long data
   */
  public void putLong(long t, long v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        longRet.add(new long[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        long[] newValueData = new long[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(booleanRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        longRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    longRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put float data.
   *
   * @param t timestamp
   * @param v float data
   */
  public void putFloat(long t, float v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        floatRet.add(new float[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        float[] newValueData = new float[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(booleanRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        floatRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    floatRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put double data.
   *
   * @param t timestamp
   * @param v double data
   */
  public void putDouble(long t, double v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        doubleRet.add(new double[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        double[] newValueData = new double[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(doubleRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        doubleRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    doubleRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put binary data.
   *
   * @param t timestamp
   * @param v binary data.
   */
  public void putBinary(long t, Binary v, int id) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        binaryRet.add(new Binary[capacity]);
        deviceIdentifierRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        Binary[] newValueData = new Binary[newCapacity];
        int[] newDeviceData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(binaryRet.get(0), 0, newValueData, 0, capacity);
        System.arraycopy(deviceIdentifierRet.get(0), 0, newDeviceData, 0, capacity);
        timeRet.set(0, newTimeData);
        binaryRet.set(0, newValueData);
        deviceIdentifierRet.set(0, newDeviceData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    binaryRet.get(writeCurListIndex)[writeCurArrayIndex] = v;
    deviceIdentifierRet.get(writeCurListIndex)[writeCurArrayIndex] = id;

    writeCurArrayIndex++;
    count++;
  }

  public int currentDeviceIdentifier() {
    return this.deviceIdentifierRet.get(readCurListIndex)[readCurArrayIndex];
  }
}
