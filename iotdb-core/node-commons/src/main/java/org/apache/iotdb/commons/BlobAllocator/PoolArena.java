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

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class PoolArena {
  private static final Logger LOGGER = LoggerFactory.getLogger(PoolArena.class);
  private final int arenaID;
  private MemoryRegion[] regions;
  public SizeClasses sizeClasses;
  private Evictor evictor;
  AtomicInteger numRegisterThread = new AtomicInteger(0);

  private int sampleCount;
  private final int EVICT_SAMPLE_COUNT = 100;

  private final Duration evictorShutdownTimeoutDuration =
      AllocatorConfig.DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
  private final Duration durationBetweenEvictionRuns =
      AllocatorConfig.DEFAULT_DURATION_BETWEEN_EVICTION_RUNS;

  public PoolArena(SizeClasses sizeClasses, int id) {
    this.sizeClasses = sizeClasses;
    this.arenaID = id;
    regions = new MemoryRegion[sizeClasses.getSizeClassNum()];

    for (int i = 0; i < regions.length; i++) {
      regions[i] = new MemoryRegion(sizeClasses.sizeIdx2size(i));
    }

    sampleCount = 0;
    startEvictor(durationBetweenEvictionRuns);
  }

  public byte[] allocate(PooledThreadLocalCache cache, int reqCapacity) {
    final int sizeIdx = sizeClasses.size2SizeIdx(reqCapacity);
    byte[] bytes = cache.allocate(reqCapacity, sizeIdx);
    if (bytes != null) return bytes;

    MemoryRegion region = regions[sizeIdx];
    bytes = region.allocate();

    if (bytes == null) {
      bytes = new byte[reqCapacity];
    }
    return bytes;
  }

  public void deallocate(byte[] bytes) {
    final int sizeIdx = sizeClasses.size2SizeIdx(bytes.length);

    MemoryRegion region = regions[sizeIdx];
    region.deallocate(bytes);
  }

  /**
   * Starts the evictor with the given delay. If there is an evictor running when this method is
   * called, it is stopped and replaced with a new evictor with the specified delay.
   *
   * <p>This method needs to be final, since it is called from a constructor. See POOL-195.
   *
   * @param delay time in milliseconds before start and between eviction runs
   */
  final void startEvictor(final Duration delay) {
    LOGGER.info("Starting evictor with delay {}", delay);
    final boolean isPositiveDelay = isPositive(delay);
    if (evictor == null) { // Starting evictor for the first time or after a cancel
      if (isPositiveDelay) { // Starting new evictor
        evictor = new Evictor();
        EvictionTimer.schedule(evictor, delay, delay);
      }
    } else if (isPositiveDelay) { // Restart
      EvictionTimer.cancel(evictor, evictorShutdownTimeoutDuration, true);
      evictor = new Evictor();
      EvictionTimer.schedule(evictor, delay, delay);
    } else { // Stopping evictor
      EvictionTimer.cancel(evictor, evictorShutdownTimeoutDuration, false);
    }
  }

  /** Stops the evictor. */
  void stopEvictor() {
    startEvictor(Duration.ofMillis(-1L));
  }

  static boolean isPositive(final Duration delay) {
    return delay != null && !delay.isNegative() && !delay.isZero();
  }

  public class Evictor implements Runnable {
    private ScheduledFuture<?> scheduledFuture;

    /** Cancels the scheduled future. */
    void cancel() {
      scheduledFuture.cancel(false);
    }

    @Override
    public void run() {
      LOGGER.info("Arena-{} running evictor", arenaID);

      // Start sampling
      for (MemoryRegion region : regions) {
        region.updateSample();
      }

      sampleCount++;
      if (sampleCount == EVICT_SAMPLE_COUNT) {
        // Evict
        for (MemoryRegion region : regions) {
          region.resize();
        }
        sampleCount = 0;
      }
    }

    /**
     * Sets the scheduled future.
     *
     * @param scheduledFuture the scheduled future.
     */
    void setScheduledFuture(final ScheduledFuture<?> scheduledFuture) {
      this.scheduledFuture = scheduledFuture;
    }

    @Override
    public String toString() {
      return getClass().getName() + " [scheduledFuture=" + scheduledFuture + "]";
    }
  }
}
