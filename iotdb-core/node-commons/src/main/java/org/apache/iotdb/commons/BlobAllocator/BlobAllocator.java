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

import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlobAllocator.class);
  private static final int INT_ZERO = 0;

  private final PoolArena[] heapArenas;
  private boolean useThreadCache;
  private WrappedThreadLocalCache threadCache;
  private int nHeapArena;
  public static final BlobAllocator DEFAULT =
      new BlobAllocator(AllocatorConfig.ARENA_NUM, 4096, false);
  public static final ArenaStrategy LEAST_USED_ARENA_STRATEGY = new LeastUsedArenaStrategy();

  public BlobAllocator(int nHeapArena, int pageSize, boolean useCacheForAllThread) {
    this.nHeapArena = nHeapArena;
    checkPositiveOrZero(nHeapArena, "nHeapArena");

    // Todo: allocate arena with remaining space
    useThreadCache = useCacheForAllThread;
    heapArenas = newArenaArray(nHeapArena);

    SizeClasses sizeClasses =
        new SizeClasses(
            AllocatorConfig.DEFAULT_MIN_BUFFER_SIZE, AllocatorConfig.DEFAULT_MAX_BUFFER_SIZE);

    for (int i = 0; i < heapArenas.length; i++) {
      PoolArena arena = new PoolArena(sizeClasses, i);
      heapArenas[i] = arena;
    }

    threadCache = new WrappedThreadLocalCache();
  }

  public byte[] allocateBlob(int reqCapacity) {
    if (reqCapacity < AllocatorConfig.DEFAULT_MIN_BUFFER_SIZE
        | reqCapacity > AllocatorConfig.DEFAULT_MAX_BUFFER_SIZE) {
      return new byte[reqCapacity];
    }

    PooledThreadLocalCache cache = threadCache.get();
    PoolArena arena = cache.arena;

    return arena.allocate(cache, reqCapacity);
  }

  public void deallocateBlob(byte[] bytes) {
    if (bytes.length >= AllocatorConfig.DEFAULT_MIN_BUFFER_SIZE
        && bytes.length <= AllocatorConfig.DEFAULT_MAX_BUFFER_SIZE) {
      PooledThreadLocalCache cache = threadCache.get();
      PoolArena arena = cache.arena;

      arena.deallocate(bytes);
    }
  }

  // TODO
  public void allocateBatch(byte[] bytes) {}

  // TODO
  public void deallocateBatch(byte[] bytes) {}

  public void enableThreadCache() {
    PooledThreadLocalCache cache = threadCache.get();
    cache.enableAllocateFromThis();
  }

  @SuppressWarnings("unchecked")
  private static PoolArena[] newArenaArray(int size) {
    return new PoolArena[size];
  }

  /**
   * Checks that the given argument is positive or zero. If it is not , throws {@link
   * IllegalArgumentException}. Otherwise, returns the argument.
   */
  public static int checkPositiveOrZero(int i, String name) {
    if (i < INT_ZERO) {
      throw new IllegalArgumentException(name + " : " + i + " (expected: >= 0)");
    }
    return i;
  }

  private final class WrappedThreadLocalCache extends FastThreadLocal<PooledThreadLocalCache> {
    @Override
    protected synchronized PooledThreadLocalCache initialValue() {
      PoolArena arena = BlobAllocator.LEAST_USED_ARENA_STRATEGY.choose(heapArenas);
      arena.numRegisterThread.incrementAndGet();
      return new PooledThreadLocalCache(arena);
    }
  }

  private static class LeastUsedArenaStrategy implements ArenaStrategy {
    @Override
    public PoolArena choose(PoolArena[] arenas) {
      if (arenas == null || arenas.length == 0) {
        return null;
      }

      PoolArena minArena = arenas[0];

      for (int i = 1; i < arenas.length; i++) {
        PoolArena arena = arenas[i];
        if (arena.numRegisterThread.get() < minArena.numRegisterThread.get()) {
          minArena = arena;
        }
      }

      return minArena;
    }
  }
}
