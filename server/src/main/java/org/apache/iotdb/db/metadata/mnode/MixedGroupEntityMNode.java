/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.mnode;

import java.util.concurrent.ConcurrentHashMap;

public class MixedGroupEntityMNode extends EntityMNode {
  /**
   * Constructor of MNode.
   *
   * @param parent
   * @param name
   */
  public MixedGroupEntityMNode(IMNode parent, String name) {
    super(parent, name);
    MixedGroupMappingNode child = new MixedGroupMappingNode(this, "");
    children = new ConcurrentHashMap<>();
    children.put("", child);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return true;
  }

  /** get the child with the name */
  @Override
  public IMNode getChild(String name) {
    return children.get("");
  }

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   */
  @Override
  public void addChild(String name, IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    children.get("").addChild(name, child);
  }

  /**
   * Add a child to the current mnode.
   *
   * <p>This method will not take the child's name as one of the inputs and will also make this
   * Mnode be child node's parent. All is to reduce the probability of mistaken by users and be more
   * convenient for users to use. And the return of this method is used to conveniently construct a
   * chain of time series for users.
   *
   * @param child child's node
   * @return return the MNode already added
   */
  @Override
  public IMNode addChild(IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    return children.get("").addChild(child);
  }
}
