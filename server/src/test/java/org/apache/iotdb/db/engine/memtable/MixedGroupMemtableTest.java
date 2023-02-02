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

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.MixedGroupPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class MixedGroupMemtableTest {
  double delta;

  @Before
  public void setUp() {
    delta = Math.pow(0.1, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }

  @Test
  public void simpleTest() throws IOException, QueryProcessException, MetadataException {
    MixedGroupingMemTable memTable = new MixedGroupingMemTable();
    int count = 10;
    String deviceId = "d1";
    String[] measurementId = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }

    int dataSize = 10;
    for (int i = 0; i < dataSize; i++) {
      memTable.writeMixedGroup(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          (byte) 1,
          dataSize - i - 1,
          new Object[] {dataSize - i - 1});
      memTable.writeMixedGroup(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          (byte) 2,
          dataSize - i - 1,
          new Object[] {dataSize - i - 1});
    }
    //        for (int i = 0; i < dataSize; i++) {
    //            memTable.writeMixedGroup(DeviceIDFactory.getInstance().getDeviceID(new
    // PartialPath(deviceId)),
    //                    Collections.singletonList(
    //                            new MeasurementSchema(measurementId[0], TSDataType.INT32,
    // TSEncoding.PLAIN)),
    //                    (byte)2,
    //                    dataSize - i - 1,
    //                    new Object[] {dataSize-i-1}
    //            );
    //        }
    MeasurementPath fullPath =
        new MixedGroupPath(
            deviceId,
            measurementId[0],
            new MeasurementSchema(
                measurementId[0],
                TSDataType.INT32,
                TSEncoding.RLE,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()),
            (byte) 2);
    //        memTable.query()
    ReadOnlyMemChunk memChunk = memTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    for (int i = 0; i < dataSize; i++) {
      iterator.hasNextTimeValuePair();
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getValue());
    }
  }
}
