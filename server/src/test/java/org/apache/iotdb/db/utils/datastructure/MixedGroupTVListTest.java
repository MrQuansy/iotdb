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

package org.apache.iotdb.db.utils.datastructure;

import org.junit.Assert;
import org.junit.Test;

public class MixedGroupTVListTest {

  @Test
  public void testIntTVList1() {
    MixedGroupIntTVList tvList = new MixedGroupIntTVList();
    for (int i = 0; i < 10; i++) {
      tvList.putInt(i, i, (byte) 1);
      tvList.putInt(i, i, (byte) 2);
    }
    for (int i = 0; i < tvList.rowCount; i += 2) {
      Assert.assertEquals(i / 2, tvList.getTime(i));
      Assert.assertEquals(i / 2, tvList.getTime(i + 1));
    }
    tvList.sort();
    for (int i = 0; i < tvList.rowCount / 2; i++) {
      Assert.assertEquals(i, tvList.getInt(i));
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }
}
