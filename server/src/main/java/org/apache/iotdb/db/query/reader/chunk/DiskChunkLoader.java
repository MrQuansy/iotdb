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

package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.MixedGroupChunkReader;

import java.io.IOException;

/** To read one chunk from disk, and only used in iotdb server module */
public class DiskChunkLoader implements IChunkLoader {

  private final boolean debug;

  public DiskChunkLoader(boolean debug) {
    this.debug = debug;
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException {
    return ChunkCache.getInstance().get(chunkMetaData, debug);
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetaData, debug);
    chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
    return new ChunkReader(chunk, timeFilter);
  }

  public IChunkReader getMixedGroupChunkReader(
      IChunkMetadata chunkMetaData, Filter timeFilter, byte deviceIdentifier, boolean getAllData)
      throws IOException {
    Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetaData, debug);
    chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
    return new MixedGroupChunkReader(chunk, timeFilter, deviceIdentifier, getAllData);
  }
}
