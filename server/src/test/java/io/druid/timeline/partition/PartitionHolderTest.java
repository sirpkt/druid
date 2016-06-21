/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class PartitionHolderTest
{
  ObjectMapper mapper = new DefaultObjectMapper();
  @Test
  public void testHybrid()
  {
    ShardSpec hash = new HashBasedNumberedShardSpec(0, 1, null, mapper);
    ShardSpec hash1 = new HashBasedNumberedShardSpec(
        1,
        2,
        null,
        mapper
    );
    ShardSpec hash2 = new HashBasedNumberedShardSpec(
        2,
        2,
        null,
        mapper
    );
    PartitionHolder holder = new PartitionHolder(hash.createChunk(new Object()));
    holder.add(hash1.createChunk(new Object()));
    holder.add(hash2.createChunk(new Object()));

    Assert.assertTrue(holder.isComplete());
  }
}
