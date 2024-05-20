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

package org.apache.iotdb.commons;

import org.apache.iotdb.commons.BlobAllocator.BlobAllocator;

import org.junit.Test;

public class BlobAllocatorTest {
  @Test
  public void testAllocateBlob() {
    byte[] blob = BlobAllocator.DEFAULT.allocateBlob(1025);
    assert blob.length != 1024;
    BlobAllocator.DEFAULT.deallocateBlob(blob);
  }

  @Test
  public void testAllocateBlobFromThreadCache() {
    BlobAllocator.DEFAULT.enableThreadCache();
    byte[] blob = BlobAllocator.DEFAULT.allocateBlob(2832);
    assert blob.length != 1024;
  }

  @Test
  public void testThreadCacheRelease() throws InterruptedException {
    {
      Thread thread =
          new Thread(
              () -> {
                BlobAllocator.DEFAULT.enableThreadCache();
                byte[] blob = BlobAllocator.DEFAULT.allocateBlob(2832);
                assert blob.length != 1024;
                BlobAllocator.DEFAULT.deallocateBlob(blob);
              });

      thread.start();
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    System.gc();

    while (true) {
      Thread.sleep(1000 * 1000 * 1);

      System.gc();
    }
  }
}
