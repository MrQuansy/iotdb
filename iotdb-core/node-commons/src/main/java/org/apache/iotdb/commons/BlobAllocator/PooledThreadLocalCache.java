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

import io.netty.util.internal.PlatformDependent;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class PooledThreadLocalCache {
  public final PoolArena arena;
  private boolean enableAllocateFromThis;
  private TCacheSlabBin[] tCacheSlabBins;
  private static final int slabBinCapacity = 100;

  @SuppressWarnings("unused") // Field is only here for the finalizer.
  private final PooledThreadLocalCache.FreeOnFinalize freeOnFinalize;

  private AtomicBoolean freed = new AtomicBoolean(false);

  PooledThreadLocalCache(PoolArena arena) {
    this.arena = arena;
    freeOnFinalize = new PooledThreadLocalCache.FreeOnFinalize(this);
    enableAllocateFromThis = false;
  }

  public void enableAllocateFromThis() {
    enableAllocateFromThis = true;
    tCacheSlabBins = new TCacheSlabBin[arena.sizeClasses.getSizeClassNum()];
  }

  public boolean add(byte[] bytes) {
    int sizeIdx = arena.sizeClasses.size2SizeIdx(bytes.length);
    if (tCacheSlabBins[sizeIdx] == null) {
      tCacheSlabBins[sizeIdx] = new TCacheSlabBin(arena, bytes.length, slabBinCapacity);
    }

    if (freed.get()) {
      return false;
    }
    return tCacheSlabBins[sizeIdx].add(bytes);
  }

  public byte[] allocate(int reqCapacity, int sizeIdx) {
    if (!enableAllocateFromThis) return null;

    if (tCacheSlabBins[sizeIdx] == null) {
      tCacheSlabBins[sizeIdx] = new TCacheSlabBin(arena, reqCapacity, slabBinCapacity);
    }
    return tCacheSlabBins[sizeIdx].allocate();
  }

  public void free() {
    arena.numRegisterThread.decrementAndGet();
    if (!enableAllocateFromThis) return;

    for (TCacheSlabBin slabBin : tCacheSlabBins) {
      if (slabBin != null) {
        slabBin.free();
      }
    }
  }

  private static class TCacheSlabBin {
    private final int size;
    private final int capacity;
    private final Queue<byte[]> queue;
    private int allocations;
    private PoolArena arena;

    public TCacheSlabBin(PoolArena arena, int size, int capacity) {
      this.arena = arena;
      this.size = size;
      this.capacity = capacity;
      queue = PlatformDependent.newFixedMpscQueue(this.capacity);
      allocations = 0;
    }

    /** Allocate something out of the cache if possible and remove the entry from the cache. */
    public byte[] allocate() {
      byte[] buf = queue.poll();
      if (buf == null) {
        return null;
      }

      // allocations is not thread-safe which is fine as this is only called from the same thread
      // all time.
      ++allocations;
      return buf;
    }

    /** Add to cache if not already full. */
    public boolean add(byte[] bytes) {
      boolean queued = queue.offer(bytes);
      if (!queued) {
        // update metric
      }
      return queued;
    }

    /**
     * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
     */
    public final int free() {
      return free(Integer.MAX_VALUE);
    }

    private int free(int max) {
      int numFreed = 0;
      for (; numFreed < max; numFreed++) {
        byte[] bytes = queue.poll();
        if (bytes != null) {
          arena.deallocate(bytes);
        } else {
          // all cleared
          return numFreed;
        }
      }
      return numFreed;
    }
  }

  private static final class FreeOnFinalize {
    private final PooledThreadLocalCache cache;

    private FreeOnFinalize(PooledThreadLocalCache cache) {
      this.cache = cache;
    }

    @Override
    protected void finalize() throws Throwable {
      try {
        super.finalize();
      } finally {
        cache.free();
      }
    }
  }
}
