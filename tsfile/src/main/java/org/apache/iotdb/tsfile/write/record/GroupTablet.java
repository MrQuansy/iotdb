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

package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;

public class GroupTablet extends Tablet {

  public int[] deviceIds;

  public GroupTablet(String deviceId, List<MeasurementSchema> schemas) {
    super(deviceId, schemas);
    deviceIds = new int[DEFAULT_SIZE];
  }

  public GroupTablet(String deviceId, List<MeasurementSchema> schemas, int maxRowNumber) {
    super(deviceId, schemas, maxRowNumber);
    deviceIds = new int[maxRowNumber];
  }

  public void addDeviceId(int rowIndex, int deviceId) {
    deviceIds[rowIndex] = deviceId;
  }
}
