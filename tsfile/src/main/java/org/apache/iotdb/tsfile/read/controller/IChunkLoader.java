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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;

import java.io.IOException;

public interface IChunkLoader {

  /** read all content of any chunk. */
  Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException;

  /** close the file reader. */
  void close() throws IOException;

  IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter timeFilter) throws IOException;

  default IChunkReader getMixedGroupChunkReader(
      IChunkMetadata chunkMetaData, Filter timeFilter, byte deviceIdentifier, boolean getAllData)
      throws IOException {
    throw new RuntimeException("Not supported mixed group for aligned time series.");
  }
}
