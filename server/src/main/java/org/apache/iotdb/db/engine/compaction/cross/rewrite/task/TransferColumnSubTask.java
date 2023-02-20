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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.inner.MixedGroupCompactionUtils;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.MixedGroupBatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is used to implement reading the measurements and writing to the target files in
 * parallel in the compaction. Currently, it only works for nonAligned data in cross space
 * compaction and unseq inner space compaction.
 */
public class TransferColumnSubTask implements Callable<Void> {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private final List<TsFileResource> mixedStorageFileList;

  private final QueryContext queryContext;
  private final QueryDataSource queryDataSource;
  private AbstractCompactionWriter compactionWriter;

  private final int taskId;

  public TransferColumnSubTask(
      List<TsFileResource> mixedStorageFileList,
      QueryContext queryContext,
      QueryDataSource queryDataSource,
      int taskId) {
    this.mixedStorageFileList = mixedStorageFileList;
    this.queryContext = queryContext;
    this.queryDataSource = queryDataSource;
    this.taskId = taskId;
  }

  @Override
  public Void call() throws Exception {
    for (TsFileResource tsFileResource : mixedStorageFileList) {
      generateColumnTsFile(tsFileResource);
    }
    return null;
  }

  private void generateColumnTsFile(TsFileResource tsFileResource) throws IOException {
    TsFileResource tmpFileResource =
        new TsFileResource(new File(tsFileResource.getTsFilePath() + ".tmp"));

    try (AbstractCompactionWriter writer = new InnerSpaceCompactionWriter(tmpFileResource)) {
      TsFileSequenceReader reader =
          FileReaderManager.getInstance().get(tsFileResource.getTsFilePath(), true);
      TsFileDeviceIterator iterator = reader.getAllDevicesIteratorWithIsAligned();
      while (iterator.hasNext()) {
        String groupId = iterator.current().left;
        Map<String, MeasurementSchema> schemaMap = iterator.currentDeviceSchema();
        Map<String, MixedGroupBatchData> deviceGroupDataCache = new HashMap<>();

        for (Map.Entry<String, MeasurementSchema> schemaEntry : schemaMap.entrySet()) {
          IBatchReader batchReader =
              MixedGroupCompactionUtils.constructReader(
                  groupId,
                  Collections.singletonList(schemaEntry.getKey()),
                  Collections.singletonList(schemaEntry.getValue()),
                  schemaMap.keySet(),
                  queryContext,
                  queryDataSource);
          deviceGroupDataCache.put(
              schemaEntry.getKey(), (MixedGroupBatchData) batchReader.nextBatch());
        }

        // transfer
        boolean endFlag = true;
        while (endFlag) {
          //
        }
      }
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }
}
