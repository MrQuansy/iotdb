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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertMixedGroupRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MixedGroupingMemTable implements IMemTable {

  /** DeviceGroupId -> chunkGroup(MeasurementId -> chunk) */
  Map<IDeviceID, IWritableMemChunkGroup> memTableMap;

  /**
   * The initial value is true because we want calculate the text data size when recover memTable!!
   */
  protected boolean disableMemControl = true;

  private static final Logger logger = LoggerFactory.getLogger(MixedGroupingMemTable.class);

  private boolean shouldFlush = false;
  private final int avgSeriesPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
  /** memory size of data points, including TEXT values */
  private long memSize = 0;
  /**
   * memory usage of all TVLists memory usage regardless of whether these TVLists are full,
   * including TEXT values
   */
  private long tvListRamCost = 0;

  private int seriesNumber = 0;

  private long totalPointsNum = 0;

  private long totalPointsNumThreshold = 0;

  private long maxPlanIndex = Long.MIN_VALUE;

  private long minPlanIndex = Long.MAX_VALUE;

  private long createdTime = System.currentTimeMillis();

  private static final String METRIC_POINT_IN = "pointsIn";

  public MixedGroupingMemTable() {
    memTableMap = new HashMap<>();
  }

  public MixedGroupingMemTable(boolean enableMemControl) {
    this.disableMemControl = !enableMemControl;
    memTableMap = new HashMap<>();
  }

  @Override
  public Map<IDeviceID, IWritableMemChunkGroup> getMemTableMap() {
    return memTableMap;
  }

  @Override
  public void write(
      IDeviceID deviceId,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {}

  @Override
  public void writeAlignedRow(
      IDeviceID deviceId,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {}

  @Override
  public void write(InsertTabletPlan insertTabletPlan, int start, int end) {}

  @Override
  public void writeAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end) {}

  @Override
  public void writeMixedGroup(
      InsertTabletPlan insertTabletPlan, int start, int end, int deviceId) {}

  @Override
  public void writeMixedGroup(
      IDeviceID groupId,
      List<IMeasurementSchema> schemaList,
      byte deviceId,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        memTableMap.computeIfAbsent(groupId, k -> new MixedGroupWritableMemChunkGroup());
    memChunkGroup.write(insertTime, objectValue, schemaList, deviceId);
  }

  @Override
  public long size() {
    long sum = 0;
    for (IWritableMemChunkGroup writableMemChunkGroup : memTableMap.values()) {
      sum += writableMemChunkGroup.count();
    }
    return sum;
  }

  @Override
  public long memSize() {
    return memSize;
  }

  @Override
  public void addTVListRamCost(long cost) {
    tvListRamCost += cost;
  }

  @Override
  public void releaseTVListRamCost(long cost) {
    tvListRamCost -= cost;
  }

  @Override
  public long getTVListsRamCost() {
    return tvListRamCost;
  }

  // todo points num threshold
  @Override
  public boolean reachTotalPointNumThreshold() {
    return false;
  }

  @Override
  public int getSeriesNumber() {
    return seriesNumber;
  }

  @Override
  public long getTotalPointsNum() {
    return totalPointsNum;
  }

  @Override
  public void insert(InsertRowPlan insertRowPlan) {
    // if this insert plan isn't from storage engine (mainly from test), we should set a temp device
    // id for it
    if (insertRowPlan.getDeviceID() == null) {
      insertRowPlan.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertRowPlan.getDevicePath()));
    }

    String[] measurements = insertRowPlan.getMeasurements();
    Object[] values = insertRowPlan.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    int nullPointsNumber = 0;
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // use values[i] to ignore null value
      if (values[i] == null) {
        nullPointsNumber++;
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    memSize += MemUtils.getMixedGroupRecordsSize(dataTypes, values, disableMemControl);
    if (insertRowPlan instanceof InsertMixedGroupRowPlan) {
      writeMixedGroup(
          insertRowPlan.getDeviceID(),
          schemaList,
          ((InsertMixedGroupRowPlan) insertRowPlan).getDeviceIdentifier(),
          insertRowPlan.getTime(),
          values);
    } else {
      write(
          insertRowPlan.getDeviceID(),
          insertRowPlan.getFailedIndices(),
          schemaList,
          insertRowPlan.getTime(),
          values);
    }

    int pointsInserted =
        insertRowPlan.getMeasurements().length
            - insertRowPlan.getFailedMeasurementNumber()
            - nullPointsNumber;

    totalPointsNum += pointsInserted;

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN,
              Tag.TYPE.toString(),
              "insertRow");
    }
  }

  @Override
  public void insertAlignedRow(InsertRowPlan insertRowPlan) {}

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {}

  @Override
  public void insertAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {}

  // todo query data in memtable
  @Override
  public ReadOnlyMemChunk query(
      PartialPath fullPath, long ttlLowerBound, List<Pair<Modification, IMemTable>> modsToMemtable)
      throws IOException, QueryProcessException, MetadataException {
    return fullPath.getReadOnlyMemChunkFromMemTable(this, modsToMemtable, ttlLowerBound);
  }

  @Override
  public void clear() {
    memTableMap.clear();
    memSize = 0;
    seriesNumber = 0;
    totalPointsNum = 0;
    totalPointsNumThreshold = 0;
    tvListRamCost = 0;
    maxPlanIndex = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  // todo delete data in memory table
  @Override
  public void delete(
      PartialPath path, PartialPath devicePath, long startTimestamp, long endTimestamp) {}

  @Override
  public IMemTable copy() {
    return null;
  }

  @Override
  public boolean isSignalMemTable() {
    return false;
  }

  @Override
  public void setShouldFlush() {
    shouldFlush = true;
  }

  @Override
  public boolean shouldFlush() {
    return shouldFlush;
  }

  @Override
  public void release() {
    for (Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      entry.getValue().release();
    }
  }

  @Override
  public boolean checkIfChunkDoesNotExist(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return true;
    }
    return !memChunkGroup.contains(measurement);
  }

  @Override
  public long getCurrentTVListSize(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    return memChunkGroup.getCurrentTVListSize(measurement);
  }

  @Override
  public void addTextDataSize(long textDataIncrement) {
    this.memSize += textDataIncrement;
  }

  @Override
  public void releaseTextDataSize(long textDataDecrement) {
    this.memSize -= textDataDecrement;
  }

  @Override
  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  @Override
  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  @Override
  public long getCreatedTime() {
    return createdTime;
  }
}
