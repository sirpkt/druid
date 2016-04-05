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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metamx.common.Pair;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;

import java.util.List;
import java.util.Map;

public class HadoopSettlingConfig
{
  final private MetadataStorageConnectorConfig config;
  final private String query;
  final private List<String> columns;
  final private String offsetColumn;
  final private String sizeColumn;
  private Map<MultiKey, Pair<Integer, Integer>> settlingMap;
  private boolean getSource = false;

  @JsonCreator
  public HadoopSettlingConfig(
    @JsonProperty(value = "connectorConfig", required = true)
    final MetadataStorageConnectorConfig connectorConfig,
    @JsonProperty(value = "query", required = true)
    final String query,
    @JsonProperty(value = "dimColumns", required = true)
    final List<String> columns,
    @JsonProperty(value = "offsetColumn", required = true)
    final String offset,
    @JsonProperty(value = "sizeColumn", required = true)
    final String size
  )
  {
    this.config = connectorConfig;
    this.query = query;
    this.columns = columns;
    this.offsetColumn = offset;
    this.sizeColumn = size;
    settlingMap = Maps.newHashMap();
  }

  public Map<MultiKey, Pair<Integer, Integer>> getSettlingMap()
  {
    fill();

    return settlingMap;
  }

  private void fill()
  {
    if (!getSource) {
      // connect through the given connector
      final Handle handle = new DBI(
          config.getConnectURI(),
          config.getUser(),
          config.getPassword()
      ).open();

      // fill the Map
      List<Map<String, Object>> results = handle.select(query);
      String[] keys = new String[columns.size()];
      for (Map<String, Object> row: results)
      {
        int idx = 0;
        for (String column: columns)
        {
          keys[idx++] = (String)row.get(column);
        }
        MultiKey key = new MultiKey(keys);
        Pair<Integer, Integer> value = new Pair<>((Integer)row.get(offsetColumn), (Integer)row.get(sizeColumn));
        settlingMap.put(key, value);
      }

      handle.close();

      getSource = true;
    }
  }
}
