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

package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MixedGroupReadOnlyMemChunk extends ReadOnlyMemChunk {

  // deletion list for this chunk
  private static final Logger logger = LoggerFactory.getLogger(MixedGroupReadOnlyMemChunk.class);
  private byte deviceIdentifier;

  public MixedGroupReadOnlyMemChunk() {
    deletionList = null;
  }

  public MixedGroupReadOnlyMemChunk(
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      TVList tvList,
      Map<String, String> props,
      int size,
      List<TimeRange> deletionList,
      byte deviceIdentifier)
      throws IOException, QueryProcessException {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    this.encoding = encoding;
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      try {
        this.floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
      } catch (NumberFormatException e) {
        logger.warn(
            "The format of MAX_POINT_NUMBER {}  is not correct."
                + " Using default float precision.",
            props.get(Encoder.MAX_POINT_NUMBER));
      }
      if (floatPrecision < 0) {
        logger.warn(
            "The MAX_POINT_NUMBER shouldn't be less than 0." + " Using default float precision {}.",
            TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
        floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      }
    }

    this.chunkData = tvList;
    this.chunkDataSize = size;
    this.deletionList = deletionList;
    this.deviceIdentifier = deviceIdentifier;
    this.chunkPointReader =
        tvList.getIterator(floatPrecision, encoding, chunkDataSize, deletionList, deviceIdentifier);
    initChunkMeta();
  }

  @Override
  public IPointReader getPointReader() {
    chunkPointReader =
        chunkData.getIterator(
            floatPrecision, encoding, chunkDataSize, deletionList, deviceIdentifier);
    return chunkPointReader;
  }
}
