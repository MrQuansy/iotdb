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

public class MixedGroupBatchData extends BatchData {

  public int[] deviceIds;

  public MixedGroupBatchData(TSDataType type) {
    super(type);
    deviceIds = new int[10000];
  }

  public void putBoolean(long t, boolean v, int deviceId) {
    deviceIds[count] = deviceId;
    super.putBoolean(t, v);
  }

  public void putInt(long t, int v, int deviceId) {
    deviceIds[count] = deviceId;
    super.putInt(t, v);
  }

  public void putLong(long t, long v, int deviceId) {
    deviceIds[count] = deviceId;
    super.putLong(t, v);
  }

  public void putFloat(long t, float v, int deviceId) {
    deviceIds[count] = deviceId;
    super.putFloat(t, v);
  }

  public void putDouble(long t, double v, int deviceId) {
    deviceIds[count] = deviceId;
    super.putDouble(t, v);
  }
}
