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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryRegion {
  private final int size;
  private final Queue<byte[]> queue;
  private AtomicInteger allocations;
  AdaptiveWeightedAverage average;

  MemoryRegion(int size) {
    this.size = size;
    this.average = new AdaptiveWeightedAverage(AllocatorConfig.ARENA_PREDICTION_WEIGHT);
    queue = new LinkedBlockingQueue<>();
    allocations = new AtomicInteger(0);
  }

  /** Allocate something out of the cache if possible and remove the entry from the cache. */
  public final byte[] allocate() {
    allocations.incrementAndGet();
    byte[] bytes = queue.poll();
    if (bytes == null) {
      return new byte[this.size];
    }
    return bytes;
  }

  public void deallocate(byte[] bytes) {
    allocations.decrementAndGet();
    queue.add(bytes);
  }

  public void updateSample() {
    average.sample(allocations.get() + queue.size());
  }

  public void resize() {
    int remain = (int) Math.ceil(average.average()) - allocations.get();
    while (remain > 0 && !queue.isEmpty()) {
      queue.poll();
      remain--;
    }
  }
}
