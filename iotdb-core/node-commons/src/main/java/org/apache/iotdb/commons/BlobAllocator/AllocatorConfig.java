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

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.time.Duration;

public class AllocatorConfig {
  public static final int DEFAULT_MIN_BUFFER_SIZE =
      CommonDescriptor.getInstance().getConfig().getMinAllocateSize();

  public static final int DEFAULT_MAX_BUFFER_SIZE =
      CommonDescriptor.getInstance().getConfig().getMaxAllocateSize();

  public static final int ARENA_NUM = CommonDescriptor.getInstance().getConfig().getArenaNum();

  /**
   * The default value for {@code evictorShutdownTimeout} configuration attribute.
   *
   * @see GenericObjectPool#getEvictorShutdownTimeoutDuration()
   * @see GenericKeyedObjectPool#getEvictorShutdownTimeoutDuration()
   * @deprecated Use {@link #DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT}.
   */
  @Deprecated public static final long DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS = 10L * 1000L;

  /**
   * The default value for {@code evictorShutdownTimeout} configuration attribute.
   *
   * @see GenericObjectPool#getEvictorShutdownTimeoutDuration()
   * @see GenericKeyedObjectPool#getEvictorShutdownTimeoutDuration()
   * @since 2.10.0
   */
  public static final Duration DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT =
      Duration.ofMillis(DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS);

  /**
   * The default value for the {@code timeBetweenEvictionRuns} configuration attribute.
   *
   * @see GenericObjectPool#getDurationBetweenEvictionRuns()
   * @see GenericKeyedObjectPool#getDurationBetweenEvictionRuns()
   * @deprecated Use {@link #DEFAULT_TIME_BETWEEN_EVICTION_RUNS}.
   */
  @Deprecated public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 10000L;

  /**
   * The default value for the {@code timeBetweenEvictionRuns} configuration attribute.
   *
   * @see GenericObjectPool#getDurationBetweenEvictionRuns()
   * @see GenericKeyedObjectPool#getDurationBetweenEvictionRuns()
   * @since 2.12.0
   */
  public static final Duration DEFAULT_DURATION_BETWEEN_EVICTION_RUNS =
      Duration.ofMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);

  public static final int ARENA_PREDICTION_WEIGHT = 35;
}
