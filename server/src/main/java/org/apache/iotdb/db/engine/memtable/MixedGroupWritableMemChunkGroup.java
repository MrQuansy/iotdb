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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MixedGroupWritableMemChunkGroup implements IWritableMemChunkGroup {

  private Map<String, IWritableMemChunk> memChunkMap;

  @Override
  public void write(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList, byte deviceId) {
    int emptyColumnCount = 0;
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IMeasurementSchema schema = schemaList.get(i - emptyColumnCount);
      IWritableMemChunk memChunk =
          memChunkMap.computeIfAbsent(
              schema.getMeasurementId(), k -> new MixedGroupWritableMemChunk(schema));
      memChunk.write(insertTime, objectValue[i], deviceId);
    }
  }

  public MixedGroupWritableMemChunkGroup() {
    memChunkMap = new HashMap<>();
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {}

  @Override
  public void release() {
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      memChunk.release();
    }
  }

  @Override
  public long count() {
    long count = 0;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      count += memChunk.count();
    }
    return count;
  }

  @Override
  public boolean contains(String measurement) {
    return memChunkMap.containsKey(measurement);
  }

  @Override
  public void write(
      long insertTime,
      Object[] objectValue,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList) {}

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    return 0;
  }

  @Override
  public long getCurrentTVListSize(String measurement) {
    return memChunkMap.get(measurement).getTVList().rowCount();
  }
}
