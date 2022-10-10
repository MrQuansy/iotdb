package org.apache.iotdb.db.engine.storagegroup;

import java.util.Map;

public interface ILastFlushTimeManagerV2 {
  //    void setMultiDeviceLastTime(long timePartitionId, Map<String, Map<String, Long>>
  // lastTimeMap);

  void setMultiDeviceLastTime(long timePartitionId, Map<String, Long> lastTimeMap);

  // void setOneDeviceLastTime(long timePartitionId, String path, Map<String, Long> lastTimeMap);

  void setOneDeviceLastTime(long timePartitionId, String path, Long time);

  //    void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Map<String, Long>>
  // flushedTimeMap);

  void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap);

  // void setOneDeviceFlushedTime(long timePartitionId, String path, Map<String, Long>
  // flushedTimeMap);

  void setOneDeviceFlushedTime(long timePartitionId, String path, long time);

  void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap);

  // void setOneDeviceGlobalFlushedTime(String path, Map<String, Long> globalFlushedTimeMap);

  void setOneDeviceGlobalFlushedTime(String path, long time);

  // region update
  void updateDeviceLastTime(long timePartitionId, String path, long time);

  void updateSeriesLastTime(long timePartitionId, String path, Map<String, Long> lastTimeMap);

  void updateDeviceFlushedTime(long timePartitionId, String path, long time);

  void updateDeviceGlobalFlushedTime(String path, long time);

  //    void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
  //            long partitionId, String deviceId, long time);
  // endregion

  // region support upgrade methods
  //    void applyNewlyFlushedTimeToFlushedTime();

  boolean updateLatestFlushTime(long partitionId);
  // endregion

  // region query
  // long getDeviceFlushedTime(long timePartitionId, String devicePath);

  long[] getSeriesFlushedTime(long timePartitionId, String devicePath, String[] measurements);

  long getDeviceFlushedTime(long timePartitionId, String devicePath);

  long getDeviceLastTime(long timePartitionId, String path);

  long[] getSeriesLastTime(long timePartitionId, String path, String[] measurements);

  // long getGlobalFlushedTime(String path);
  // endregion

  // region clear
  void clearLastTime();

  void clearFlushedTime();

  void clearGlobalFlushedTime();
  // endregion

}
