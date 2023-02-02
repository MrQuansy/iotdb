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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.MixedGroupPageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class MixedGroupChunkWriterImpl implements IChunkWriter {

  private static final Logger logger = LoggerFactory.getLogger(MixedGroupChunkWriterImpl.class);

  private final IMeasurementSchema measurementSchema;

  private final ICompressor compressor;

  /** all pages of this chunk. */
  private final PublicBAOS pageBuffer;

  private int numOfPages;

  /** write data into current page */
  private MixedGroupPageWriter pageWriter;

  /** page size threshold. */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private Statistics<? extends Serializable> statistics;

  /** first page info */
  private int sizeWithoutStatistic;

  private Statistics<?> firstPageStatistics;

  public MixedGroupChunkWriterImpl(IMeasurementSchema schema) {
    measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new MixedGroupPageWriter(measurementSchema);
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter, statistics);

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    sizeWithoutStatistic = 0;
    firstPageStatistics = null;
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  @Override
  public long getSerializedChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId(), pageBuffer.size())
        + (long) pageBuffer.size();
  }

  @Override
  public void sealCurrentPage() {
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {
      writePageToPageBuffer();
    }
  }

  private void writePageToPageBuffer() {
    try {
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = pageWriter.getStatistics();
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
        firstPageStatistics = null;
      } else {
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
      }

      // update statistics of this chunk
      numOfPages++;
      this.statistics.mergeStatistics(pageWriter.getStatistics());
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset(measurementSchema);
    }
  }

  @Override
  public void clearPageWriter() {
    pageWriter = null;
  }

  // todo
  public void writeGroupColumn(long[] timestampColumn, int[] valueColumn, byte[] deviceIdColumn) {}

  public void write(long time, int value, int deviceId) {
    pageWriter.write(time, value, deviceId);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, long value, int deviceId) {
    pageWriter.write(time, value, deviceId);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, double value, int deviceId) {
    pageWriter.write(time, value, deviceId);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, float value, int deviceId) {
    pageWriter.write(time, value, deviceId);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, boolean value, int deviceId) {
    pageWriter.write(time, value, deviceId);
    checkPageSizeAndMayOpenANewPage();
  }

  // Todo
  private void checkPageSizeAndMayOpenANewPage() {
    //        if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
    //            logger.debug("current line count reaches the upper bound, write page {}",
    // measurementSchema);
    //            writePageToPageBuffer();
    //        } else if (pageWriter.getPointNumber()
    //                >= valueCountInOnePageForNextCheck) { // need to check memory size
    //            // not checking the memory used for every value
    //            long currentPageSize = pageWriter.estimateMaxMemSize();
    //            if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
    //                // we will write the current page
    //                logger.debug(
    //                        "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{},
    // valueCountInOnePage:{}",
    //                        measurementSchema.getMeasurementId(),
    //                        pageSizeThreshold,
    //                        currentPageSize,
    //                        pageWriter.getPointNumber());
    //                writePageToPageBuffer();
    //                valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
    //            } else {
    //                // reset the valueCountInOnePageForNextCheck for the next page
    //                valueCountInOnePageForNextCheck =
    //                        (int) (((float) pageSizeThreshold / currentPageSize) *
    // pageWriter.getPointNumber());
    //            }
    //        }
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @param statistics the chunk statistics
   * @throws IOException exception in IO
   */
  private void writeAllPagesOfChunkToTsFile(
      TsFileIOWriter writer, Statistics<? extends Serializable> statistics) throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(
        measurementSchema.getMeasurementId(),
        compressor.getType(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        statistics,
        pageBuffer.size(),
        numOfPages,
        TsFileConstant.MIXED_GROUP_COLUMN_MASK);

    long dataOffset = writer.getPos();

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);

    int dataSize = (int) (writer.getPos() - dataOffset);
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size());
    }

    writer.endCurrentChunk();
  }
}
