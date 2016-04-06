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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class HadoopSettlingConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testHiveConnection() throws IOException
  {
    String meta = "{\"createTables\":true,\"connectURI\":\"jdbc:hive2://emn-g04-03:10000\",\"user\":\"hadoop\",\"password\":\"hadoop\"}";
    MetadataStorageConnectorConfig connectorConfig = jsonMapper.readValue(meta, MetadataStorageConnectorConfig.class);
    String query = "select module_name, eqp_param_name, sum_type_cd, eqp_recipe_id, eqp_step_id, lot_code, count_settling, count_activation from big_fdc_settling_info where count_settling > 0.0 and count_activation != -1.0";
    HadoopSettlingConfig settlingConfig = new HadoopSettlingConfig(
        connectorConfig,
        query,
        Arrays.asList("module_name", "eqp_param_name"),
        Arrays.asList("eqp_recipe_id", "eqp_step_id", "lot_code"),
        "sum_type_cd",
        "count_settling",
        "count_activation"
    );
    Assert.assertTrue(settlingConfig.mapSize() > 0);
  }
}
