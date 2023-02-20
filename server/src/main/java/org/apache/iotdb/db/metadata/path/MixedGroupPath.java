/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.MixedGroupReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MixedGroupPath extends MeasurementPath {

  private static final Logger logger = LoggerFactory.getLogger(MixedGroupPath.class);

  private byte deviceIdentifier;

  private boolean isGetAllData = false;

  public MixedGroupPath(byte deviceIdentifier) {
    this.deviceIdentifier = deviceIdentifier;
  }

  public MixedGroupPath(String measurementPath, byte deviceIdentifier) throws IllegalPathException {
    super(measurementPath);
    this.deviceIdentifier = deviceIdentifier;
  }

  public MixedGroupPath(String measurementPath, TSDataType type, byte deviceIdentifier)
      throws IllegalPathException {
    super(measurementPath, type);
    this.deviceIdentifier = deviceIdentifier;
  }

  public MixedGroupPath(
      PartialPath measurementPath, IMeasurementSchema measurementSchema, byte deviceIdentifier) {
    super(measurementPath, measurementSchema);
    this.deviceIdentifier = deviceIdentifier;
  }

  public MixedGroupPath(
      String device,
      String measurement,
      IMeasurementSchema measurementSchema,
      byte deviceIdentifier)
      throws IllegalPathException {
    super(device, measurement, measurementSchema);
    this.deviceIdentifier = deviceIdentifier;
  }

  // For compaction
  public MixedGroupPath(
      String device, String measurement, IMeasurementSchema measurementSchema, boolean isGetAllData)
      throws IllegalPathException {
    super(device, measurement, measurementSchema);
    this.isGetAllData = isGetAllData;
  }

  public byte getDeviceIdentifier() {
    return deviceIdentifier;
  }

  @Override
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound)
      throws QueryProcessException, IOException {
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(getDevicePath());
    // check If Memtable Contains this path
    if (!memTableMap.containsKey(deviceID)
        || !memTableMap.get(deviceID).contains(getMeasurement())) {
      return null;
    }
    IWritableMemChunk memChunk = memTableMap.get(deviceID).getMemChunkMap().get(getMeasurement());
    // get sorted tv list is synchronized so different query can get right sorted list reference
    TVList chunkCopy = memChunk.getSortedTvListForQuery();
    int curSize = chunkCopy.rowCount();
    List<TimeRange> deletionList = null;
    if (modsToMemtable != null) {
      deletionList = constructDeletionList(memTable, modsToMemtable, timeLowerBound);
    }
    return new MixedGroupReadOnlyMemChunk(
        getMeasurement(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        chunkCopy,
        measurementSchema.getProps(),
        curSize,
        deletionList,
        deviceIdentifier);
  }

  /**
   * construct a deletion list from a memtable.
   *
   * @param memTable memtable
   * @param timeLowerBound time water mark
   */
  private List<TimeRange> constructDeletionList(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPath().matchFullPath(this) && deletion.getEndTime() > timeLowerBound) {
          long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
          deletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }

  @Override
  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    return new SeriesReader(
        this,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
  }

  @TestOnly
  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    allSensors.add(this.getMeasurement());
    return new SeriesReader(
        this,
        allSensors,
        dataType,
        context,
        seqFileResource,
        unseqFileResource,
        timeFilter,
        valueFilter,
        ascending);
  }

  @Override
  public TsFileResource createTsFileResource(
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    TsFileResource tsFileResource =
        new TsFileResource(this, readOnlyMemChunk, chunkMetadataList, originTsFileResource);
    tsFileResource.setTimeSeriesMetadata(
        this, generateTimeSeriesMetadata(readOnlyMemChunk, chunkMetadataList));
    return tsFileResource;
  }

  public boolean isGetAllData() {
    return isGetAllData;
  }

  public void setGetAllData(boolean getAllData) {
    isGetAllData = getAllData;
  }

  /**
   * Because the unclosed tsfile don't have TimeSeriesMetadata and memtables in the memory don't
   * have chunkMetadata, but query will use these, so we need to generate it for them.
   */
  @Override
  public ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
    timeSeriesMetadata.setMeasurementId(measurementSchema.getMeasurementId());
    timeSeriesMetadata.setTSDataType(measurementSchema.getType());
    timeSeriesMetadata.setOffsetOfChunkMetaDataList(-1);
    timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

    Statistics<? extends Serializable> seriesStatistics =
        Statistics.getStatsByType(timeSeriesMetadata.getTSDataType());
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    for (ReadOnlyMemChunk memChunk : readOnlyMemChunk) {
      if (!memChunk.isEmpty()) {
        seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
      }
    }
    timeSeriesMetadata.setStatistics(seriesStatistics);
    return timeSeriesMetadata;
  }
}
