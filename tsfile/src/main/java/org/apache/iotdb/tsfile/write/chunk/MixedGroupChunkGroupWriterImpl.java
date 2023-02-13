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

package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.GroupTablet;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MixedGroupChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MixedGroupChunkGroupWriterImpl.class);

  String groupId;

  private Map<String, MixedGroupChunkWriterImpl> groupChunkWriters = new LinkedHashMap<>();

  public MixedGroupChunkGroupWriterImpl(String groupId) {
    this.groupId = groupId;
  }

  @Override
  public int write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
    return 0;
  }

  public int write(long time, List<DataPoint> data, byte deviceIdentifier) {
    int pointCount = 0;
    for (DataPoint point : data) {
      if (pointCount == 0) {
        pointCount++;
      }
      point.writeTo(
          time,
          deviceIdentifier,
          groupChunkWriters.get(point.getMeasurementId())); // write time and value to page
    }
    return pointCount;
  }

  @Override
  public int write(Tablet tablet) throws WriteProcessException, IOException {
    List<MeasurementSchema> timeseries = tablet.getSchemas();
    GroupTablet groupTablet = (GroupTablet) tablet;
    for (int row = 0; row < tablet.rowSize; row++) {
      long time = tablet.timestamps[row];
      int deviceId = groupTablet.deviceIds[row];
      for (int column = 0; column < timeseries.size(); column++) {
        String measurementId = timeseries.get(column).getMeasurementId();
        switch (timeseries.get(column).getType()) {
          case INT32:
            groupChunkWriters
                .get(measurementId)
                .write(time, ((int[]) tablet.values[column])[row], deviceId);
            break;
          case INT64:
            groupChunkWriters
                .get(measurementId)
                .write(time, ((long[]) tablet.values[column])[row], deviceId);
            break;
          case FLOAT:
            groupChunkWriters
                .get(measurementId)
                .write(time, ((float[]) tablet.values[column])[row], deviceId);
            break;
          case DOUBLE:
            groupChunkWriters
                .get(measurementId)
                .write(time, ((double[]) tablet.values[column])[row], deviceId);
            break;
          case BOOLEAN:
            groupChunkWriters
                .get(measurementId)
                .write(time, ((boolean[]) tablet.values[column])[row], deviceId);
            break;
          case TEXT:
            //                        chunkWriters.get(measurementId).write(time, ((Binary[])
            // tablet.values[column])[row]);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", timeseries.get(column).getType()));
        }
      }
    }
    return tablet.rowSize;
  }

  @Override
  public long flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    LOG.debug("start flush device id:{}", groupId);
    // make sure all the pages have been compressed into buffers, so that we can get correct
    // groupWriter.getCurrentChunkGroupSize().
    sealAllChunks();
    long currentChunkGroupSize = getCurrentChunkGroupSize();
    for (IChunkWriter seriesWriter : groupChunkWriters.values()) {
      seriesWriter.writeToFileWriter(tsfileWriter);
    }
    return currentChunkGroupSize;
  }

  @Override
  public long updateMaxGroupMemSize() {
    long bufferSize = 0;
    for (IChunkWriter seriesWriter : groupChunkWriters.values()) {
      bufferSize += seriesWriter.estimateMaxSeriesMemSize();
    }
    return bufferSize;
  }

  @Override
  public void tryToAddSeriesWriter(MeasurementSchema measurementSchema) throws IOException {
    if (!groupChunkWriters.containsKey(measurementSchema.getMeasurementId())) {
      this.groupChunkWriters.put(
          measurementSchema.getMeasurementId(), new MixedGroupChunkWriterImpl(measurementSchema));
    }
  }

  @Override
  public void tryToAddSeriesWriter(List<MeasurementSchema> schemas) throws IOException {
    for (IMeasurementSchema schema : schemas) {
      if (!groupChunkWriters.containsKey(schema.getMeasurementId())) {
        this.groupChunkWriters.put(
            schema.getMeasurementId(), new MixedGroupChunkWriterImpl(schema));
      }
    }
  }

  @Override
  public long getCurrentChunkGroupSize() {
    long size = 0;
    for (IChunkWriter writer : groupChunkWriters.values()) {
      size += writer.getSerializedChunkSize();
    }
    return size;
  }

  /** seal all the chunks which may has un-sealed pages in force. */
  private void sealAllChunks() {
    for (IChunkWriter writer : groupChunkWriters.values()) {
      writer.sealCurrentPage();
    }
  }
}
