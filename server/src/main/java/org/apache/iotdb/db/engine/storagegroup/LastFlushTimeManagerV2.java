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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class manages last time and flush time for sequence and unsequence determination This class
 * This class is NOT thread safe, caller should ensure synchronization
 */
public class LastFlushTimeManagerV2 implements ILastFlushTimeManagerV2 {
  private static final Logger logger = LoggerFactory.getLogger(LastFlushTimeManagerV2.class);
  /*
   * time partition id -> map, which contains
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update partitionLatestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<Long, Map<String, Pair<Long, Map<String, Long>>>> latestTimeForEachSeries =
      new HashMap<>();
  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a
   * data point should be put into a sequential file or an unsequential file. Data of some device
   * with timestamp less than or equals to the device's latestFlushedTime should go into an
   * unsequential file.
   */
  private Map<Long, Map<String, Pair<Long, Map<String, Long>>>>
      partitionLatestFlushedTimeForEachSeries = new HashMap<>();
  /** used to record the latest flush time while upgrading and inserting */
  private Map<Long, Map<String, Map<String, Long>>>
      newlyFlushedPartitionLatestFlushedTimeForEachSeries = new HashMap<>();
  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with
   * partitionLatestFlushedTimeForEachDevice
   */
  private Map<String, Long> globalLatestFlushedTimeForEachSeries = new HashMap<>();

  @Override
  public void setMultiDeviceLastTime(long timePartitionId, Map<String, Long> lastTimeMap) {
    for (Entry<String, Long> entry : lastTimeMap.entrySet()) {
      latestTimeForEachSeries
          .computeIfAbsent(timePartitionId, l -> new HashMap<>())
          .put(entry.getKey(), new Pair<>(entry.getValue(), null));
    }
  }

  @Override
  public void setOneDeviceLastTime(long timePartitionId, String path, Long time) {
    latestTimeForEachSeries
        .computeIfAbsent(timePartitionId, l -> new HashMap<>())
        .put(path, new Pair<>(time, null));
  }

  @Override
  public void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap) {
    for (Entry<String, Long> entry : flushedTimeMap.entrySet()) {
      partitionLatestFlushedTimeForEachSeries
          .computeIfAbsent(timePartitionId, l -> new HashMap<>())
          .put(entry.getKey(), new Pair<>(entry.getValue(), null));
    }
  }

  @Override
  public void setOneDeviceFlushedTime(long timePartitionId, String path, long time) {
    partitionLatestFlushedTimeForEachSeries
        .computeIfAbsent(timePartitionId, l -> new HashMap<>())
        .put(path, new Pair<>(time, null));
  }

  @Override
  public void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap) {
    // todo
    // for last cache
  }

  @Override
  public void setOneDeviceGlobalFlushedTime(String path, long time) {
    // todo
    // for last cache
  }

  @Override
  public void updateDeviceLastTime(long timePartitionId, String path, long time) {
    Pair<Long, Map<String, Long>> deviceInfo =
        latestTimeForEachSeries
            .computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .computeIfAbsent(path, p -> new Pair<>(time, null));
    if (deviceInfo.left < time) {
      deviceInfo.left = time;
    }
  }

  @Override
  public void updateSeriesLastTime(
      long timePartitionId, String path, Map<String, Long> lastTimeMap) {
    Pair<Long, Map<String, Long>> deviceInfo =
        latestTimeForEachSeries
            .computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .computeIfAbsent(path, p -> new Pair<>(Long.MAX_VALUE, null));

    Map<String, Long> seriesFlushTimeMap = deviceInfo.right = new HashMap<>();
    for (Entry<String, Long> entry : lastTimeMap.entrySet()) {
      seriesFlushTimeMap.compute(
          entry.getKey(), (k, v) -> v == null ? entry.getValue() : Math.max(entry.getValue(), v));
    }
  }

  @Override
  public void updateDeviceFlushedTime(long timePartitionId, String path, long time) {
    Pair<Long, Map<String, Long>> deviceInfo =
        partitionLatestFlushedTimeForEachSeries
            .computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .computeIfAbsent(path, p -> new Pair<>(time, null));
    if (deviceInfo.left < time) {
      deviceInfo.left = time;
    }
  }

  @Override
  public void updateDeviceGlobalFlushedTime(String path, long time) {}

  @Override
  public boolean updateLatestFlushTime(long partitionId) {
    // update the largest timestamp in the last flushing memtable
    Map<String, Pair<Long, Map<String, Long>>> curPartitionDeviceLatestTime =
        latestTimeForEachSeries.get(partitionId);

    if (curPartitionDeviceLatestTime == null) {
      return false;
    }

    for (Entry<String, Pair<Long, Map<String, Long>>> entry :
        curPartitionDeviceLatestTime.entrySet()) {
      partitionLatestFlushedTimeForEachSeries
          .computeIfAbsent(partitionId, id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }
    return true;
  }

  @Override
  public long[] getSeriesFlushedTime(
      long timePartitionId, String devicePath, String[] measurements) {
    long[] result = new long[measurements.length];
    Pair<Long, Map<String, Long>> deviceFlushTimeInfo =
        partitionLatestFlushedTimeForEachSeries
            .computeIfAbsent(timePartitionId, l -> new HashMap<>())
            .get(devicePath);
    if (deviceFlushTimeInfo.right == null) {
      Arrays.fill(result, deviceFlushTimeInfo.left);
      return result;
    }

    for (int i = 0; i < measurements.length; i++) {
      if (deviceFlushTimeInfo.right.containsKey(measurements[i])) {
        result[i] = deviceFlushTimeInfo.right.get(measurements[i]);
      } else {
        result[i] = deviceFlushTimeInfo.left;
      }
    }

    return result;
  }

  @Override
  public long getDeviceFlushedTime(long timePartitionId, String devicePath) {
    return partitionLatestFlushedTimeForEachSeries.get(timePartitionId).get(devicePath).left;
  }

  @Override
  public long getDeviceLastTime(long timePartitionId, String path) {
    return latestTimeForEachSeries.get(timePartitionId).get(path).left;
  }

  @Override
  public long[] getSeriesLastTime(long timePartitionId, String devicePath, String[] measurements) {
    long[] result = new long[measurements.length];
    Pair<Long, Map<String, Long>> deviceFlushTimeInfo =
        latestTimeForEachSeries
            .computeIfAbsent(timePartitionId, l -> new HashMap<>())
            .get(devicePath);
    if (deviceFlushTimeInfo.right == null) {
      Arrays.fill(result, deviceFlushTimeInfo.left);
      return result;
    }

    for (int i = 0; i < measurements.length; i++) {
      if (deviceFlushTimeInfo.right.containsKey(measurements[i])) {
        result[i] = deviceFlushTimeInfo.right.get(measurements[i]);
      } else {
        result[i] = deviceFlushTimeInfo.left;
      }
    }

    return result;
  }

  @Override
  public void clearLastTime() {
    latestTimeForEachSeries.clear();
  }

  @Override
  public void clearFlushedTime() {
    partitionLatestFlushedTimeForEachSeries.clear();
  }

  @Override
  public void clearGlobalFlushedTime() {
    globalLatestFlushedTimeForEachSeries.clear();
  }
}
