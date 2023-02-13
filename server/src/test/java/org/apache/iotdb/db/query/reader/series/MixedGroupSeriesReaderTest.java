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

package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MixedGroupPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MixedGroupSeriesReaderTest {

  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";
  private List<String> deviceIds = new ArrayList<>();
  private List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    MixedGroupSeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unseqResources);
  }

  @After
  public void tearDown() throws IOException {
    MixedGroupSeriesReaderTestUtil.tearDown(seqResources, unseqResources);
  }

  @Test
  public void batchTest() {
    try {
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      SeriesReader seriesReader =
          new SeriesReader(
              new MixedGroupPath(SERIES_READER_TEST_SG + ".device0.sensor0", (byte) 1),
              allSensors,
              TSDataType.INT32,
              EnvironmentUtils.TEST_QUERY_CONTEXT,
              seqResources,
              unseqResources,
              null,
              null,
              true);
      IBatchReader batchReader = new SeriesRawDataBatchReader(seriesReader);
      int count = 0;
      while (batchReader.hasNextBatch()) {
        BatchData batchData = batchReader.nextBatch();
        assertEquals(TSDataType.INT32, batchData.getDataType());
        assertEquals(10, batchData.length());
        for (int i = 0; i < batchData.length(); i++) {
          long expectedTime = i + 10 * count;
          int expectedValue = i + 10 * count;
          assertEquals(expectedTime, batchData.currentTime());
          assertEquals(expectedValue, batchData.getInt());
          batchData.next();
        }
        count++;
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void batchTestWithTimeFilter() {
    try {
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      SeriesReaderByTimestamp seriesReader =
          new SeriesReaderByTimestamp(
              new MixedGroupPath(SERIES_READER_TEST_SG + ".device0.sensor0", (byte) 1),
              allSensors,
              TSDataType.INT32,
              EnvironmentUtils.TEST_QUERY_CONTEXT,
              seqResources,
              unseqResources,
              true);
      long timestamps[] = new long[500];
      for (int i = 0; i < 500; i++) {
        timestamps[i] = i;
      }
      Object[] values = seriesReader.getValuesInTimestamps(timestamps, timestamps.length);

      for (int time = 0; time < 500; time++) {
        if (time < 50) {
          Assert.assertEquals(time, values[time]);
        } else {
          Assert.assertNull(values[time]);
        }
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
