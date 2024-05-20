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
package org.apache.iotdb.commons.BlobAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BlobObjectManager {
  private static final Logger logger = LoggerFactory.getLogger(BlobObjectManager.class);

  private ConcurrentHashMap<String, SingleRegionFixedBlobPool> poolMap = new ConcurrentHashMap<>();

  private BlobObjectManager() {}

  public byte[] allocatedBlob(String devicePath, int size) {
    SingleRegionFixedBlobPool pool =
        poolMap.computeIfAbsent(devicePath, k -> new SingleRegionFixedBlobPool());
    return pool.allocate(size);
  }

  public SingleRegionFixedBlobPool getPool(String devicePath) {
    return poolMap.computeIfAbsent(devicePath, k -> new SingleRegionFixedBlobPool());
  }

  public void releaseBlob(String devicePath, byte[] blob) {
    SingleRegionFixedBlobPool pool = poolMap.get(devicePath);
    if (pool == null) {
      logger.error("No pool for devicePath: {}", devicePath);
      return;
    }
    pool.release(blob);
  }

  public static BlobObjectManager getInstance() {
    return BlobObjectManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static BlobObjectManager instance = new BlobObjectManager();
  }
}
