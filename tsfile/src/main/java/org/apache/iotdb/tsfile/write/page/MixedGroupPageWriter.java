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

package org.apache.iotdb.tsfile.write.page;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class MixedGroupPageWriter {

  private static final Logger logger = LoggerFactory.getLogger(MixedGroupPageWriter.class);

  private ICompressor compressor;

  // time
  private Encoder timeEncoder;

  private Encoder deviceColumnEncoder;

  private PublicBAOS deviceColumnOut;

  private PublicBAOS timeOut;
  // value
  private Encoder valueEncoder;
  private PublicBAOS valueOut;

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<? extends Serializable> statistics;

  public MixedGroupPageWriter(IMeasurementSchema measurementSchema) {
    this(measurementSchema.getTimeEncoder(), measurementSchema.getValueEncoder());
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
    this.compressor = ICompressor.getCompressor(measurementSchema.getCompressor());
  }

  private MixedGroupPageWriter(Encoder timeEncoder, Encoder valueEncoder) {
    this.timeOut = new PublicBAOS();
    this.valueOut = new PublicBAOS();
    this.deviceColumnOut = new PublicBAOS();
    this.timeEncoder = timeEncoder;
    this.valueEncoder = valueEncoder;
    this.deviceColumnEncoder = new DeltaBinaryEncoder.IntDeltaEncoder();
  }

  /** write a time value pair into encoder */
  public void write(long time, boolean value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, short value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, int value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, long value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, float value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, double value, int deviceId) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    deviceColumnEncoder.encode(deviceId, deviceColumnOut);
    statistics.update(time, value);
  }

  public int writePageHeaderAndDataIntoBuff(PublicBAOS pageBuffer, boolean first)
      throws IOException {
    if (statistics.getCount() == 0) {
      return 0;
    }

    ByteBuffer pageData = getUncompressedBytes();
    int uncompressedSize = pageData.remaining();
    int compressedSize;
    byte[] compressedBytes = null;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = uncompressedSize;
    } else if (compressor.getType().equals(CompressionType.GZIP)) {
      compressedBytes =
          compressor.compress(pageData.array(), pageData.position(), uncompressedSize);
      compressedSize = compressedBytes.length;
    } else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =
          compressor.compress(
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter
    int sizeWithoutStatistic = 0;
    if (first) {
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
    } else {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
      statistics.serialize(pageBuffer);
    }

    // write page content to temp PBAOS
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else {
      pageBuffer.write(compressedBytes, 0, compressedSize);
    }
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    return sizeWithoutStatistic;
  }

  /** reset this page */
  public void reset(IMeasurementSchema measurementSchema) {
    timeOut.reset();
    valueOut.reset();
    deviceColumnOut.reset();
    statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage() throws IOException {
    timeEncoder.flush(timeOut);
    valueEncoder.flush(valueOut);
    deviceColumnEncoder.flush(deviceColumnOut);
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes() throws IOException {
    prepareEndWriteOnePage();
    ByteBuffer buffer =
        ByteBuffer.allocate(timeOut.size() + valueOut.size() + deviceColumnOut.size() + 8);
    // ByteBuffer buffer = ByteBuffer.allocate(timeOut.size() + valueOut.size() + 4);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(timeOut.size(), buffer);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(valueOut.size(), buffer);
    buffer.put(timeOut.getBuf(), 0, timeOut.size());
    buffer.put(valueOut.getBuf(), 0, valueOut.size());
    buffer.put(deviceColumnOut.getBuf(), 0, deviceColumnOut.size());
    buffer.flip();
    return buffer;
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value
   * outputStream, because size outputStream is never used until flushing.
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() {
    return timeOut.size()
        + valueOut.size()
        + deviceColumnOut.size()
        + timeEncoder.getMaxByteSize()
        + valueEncoder.getMaxByteSize()
        + deviceColumnEncoder.getMaxByteSize();
  }

  public long getPointNumber() {
    return statistics.getCount();
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }
}
