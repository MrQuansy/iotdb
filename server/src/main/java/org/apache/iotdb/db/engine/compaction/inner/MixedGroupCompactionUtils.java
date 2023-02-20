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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.TransferColumnSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MixedGroupPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.MixedGroupBatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MixedGroupCompactionUtils {

  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public static void compact(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException, InterruptedException, StorageEngineException, MetadataException {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFileResources, unseqFileResources);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

    int actualSubTaskMum = Math.min(seqFileResources.size(), subTaskNum);
    List<TsFileResource>[] subTaskFile = new List[actualSubTaskMum];
    for (int i = 0; i < seqFileResources.size(); i++) {
      int index = i % actualSubTaskMum;
      if (subTaskFile[index] == null) {
        subTaskFile[index] = new ArrayList<>();
        subTaskFile[index].add(seqFileResources.get(i));
      }
    }

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < actualSubTaskMum; i++) {
      futures.add(
          CompactionTaskManager.getInstance()
              .submitSubTask(
                  new TransferColumnSubTask(subTaskFile[i], queryContext, queryDataSource, i)));
    }

    // wait for all sub tasks finish
    for (int i = 0; i < actualSubTaskMum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("SubCompactionTask meet errors ", e);
        Thread.interrupted();
        throw new InterruptedException();
      }
    }
    //        try (AbstractCompactionWriter compactionWriter =
    //                     getCompactionWriter(seqFileResources, unseqFileResources,
    // targetFileResources)) {
    //            // Do not close device iterator, because tsfile reader is managed by
    // FileReaderManager.
    //            MultiTsFileDeviceIterator deviceIterator =
    //                    new MultiTsFileDeviceIterator(seqFileResources, unseqFileResources);
    //            while (deviceIterator.hasNextDevice()) {
    //                //checkThreadInterrupted(targetFileResources);
    //                Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
    //                String device = deviceInfo.left;
    //                QueryUtils.fillOrderIndexes(queryDataSource, device, true);
    //                compactMixedGroupSeries(device, deviceIterator, compactionWriter,
    // queryContext, queryDataSource);
    //            }

    // compactionWriter.endFile();
    //        } finally {
    //            QueryResourceManager.getInstance().endQuery(queryId);
    //        }
  }

  private static AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new CrossSpaceCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new InnerSpaceCompactionWriter(targetFileResources.get(0));
    }
  }

  private static void checkThreadInterrupted(List<TsFileResource> tsFileResource)
      throws InterruptedException {
    if (Thread.interrupted() || !IoTDB.activated) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", tsFileResource.toString()));
    }
  }

  private static void compactMixedGroupSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();

    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
    if (measurementSchemas.isEmpty()) {
      return;
    }
    List<String> allMeasurements = new ArrayList<>(schemaMap.keySet());
    allMeasurements.sort((String::compareTo));
    IBatchReader dataBatchReader =
        constructReader(
            device,
            allMeasurements,
            measurementSchemas,
            schemaMap.keySet(),
            queryContext,
            queryDataSource);

    int groupPos = 0;
    int deviceId;
    if (dataBatchReader.hasNextBatch()) {
      MixedGroupBatchData batchData = (MixedGroupBatchData) dataBatchReader.nextBatch();
      //            batchData.set
      batchData.setPosition(groupPos);
      deviceId = batchData.currentDeviceIdentifier();
      //            compactionWriter.write(gr);
    }
    //        if (dataBatchReader.hasNextBatch()) {
    //            // chunkgroup is serialized only when at least one timeseries under this device
    // has data
    //            compactionWriter.startChunkGroup(device, true);
    //            compactionWriter.startMeasurement(measurementSchemas, 0);
    //            writeWithReader(compactionWriter, dataBatchReader, 0);
    //            compactionWriter.endMeasurement(0);
    //            compactionWriter.endChunkGroup();
    //        }
    //        compactionWriter.checkAndMayFlushChunkMetadata();
  }

  /**
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  public static IBatchReader constructReader(
      String deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IllegalPathException {
    PartialPath seriesPath;
    TSDataType tsDataType;
    seriesPath =
        new MixedGroupPath(deviceId, measurementIds.get(0), measurementSchemas.get(0), true);
    tsDataType = measurementSchemas.get(0).getType();
    return new SeriesRawDataBatchReader(
        seriesPath, allSensors, tsDataType, queryContext, queryDataSource, null, null, null, true);
  }
}
