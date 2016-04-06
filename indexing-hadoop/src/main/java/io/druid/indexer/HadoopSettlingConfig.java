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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.range.RangeAggregatorFactory;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.Map;

public class HadoopSettlingConfig
{
  final private MetadataStorageConnectorConfig config;
  final private String query;
  final private List<String> constColumns;
  final private List<String> regexColumns;
  final private String aggTypeColumn;
  final private String offsetColumn;
  final private String sizeColumn;

  private Map<MultiKey, Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>>> settlingMap;
  private boolean getSource = false;
  private String[] dimValues;
  private String[] regexValues;
  private HadoopSettlingMatcherFactory matcherFactory;

  @JsonCreator
  public HadoopSettlingConfig(
    @JsonProperty(value = "connectorConfig", required = true)
    final MetadataStorageConnectorConfig connectorConfig,
    @JsonProperty(value = "query", required = true)
    final String query,
    @JsonProperty(value = "constColumns", required = true)
    final List<String> constColumns,
    @JsonProperty(value = "regexColumns", required = true)
    final List<String> regexColumns,
    @JsonProperty(value = "typeColumn", required = true)
    final String aggTypeColumn,
    @JsonProperty(value = "offsetColumn", required = true)
    final String offset,
    @JsonProperty(value = "sizeColumn", required = true)
    final String size
  )
  {
    this.config = Preconditions.checkNotNull(connectorConfig);
    this.query = Preconditions.checkNotNull(query);
    this.constColumns = Preconditions.checkNotNull(constColumns);
    this.regexColumns = Preconditions.checkNotNull(regexColumns);
    this.aggTypeColumn = Preconditions.checkNotNull(aggTypeColumn);
    this.offsetColumn = Preconditions.checkNotNull(offset);
    this.sizeColumn = Preconditions.checkNotNull(size);
    settlingMap = Maps.newHashMap();
    dimValues = new String[constColumns.size()];
    regexValues = new String[regexColumns.size()];
    matcherFactory = new HadoopSettlingMatcherFactory();

    // fill the Map in advance
    fillMap();
  }

  public int mapSize()
  {
    return settlingMap.size();
  }

  public void applySettling(InputRow row, AggregatorFactory[] org, AggregatorFactory[] applied)
  {
    Map<String, Pair<Integer, Integer>> mapForRow = getValueMap(row);

    if (mapForRow != null)
    {
      // special treat for mean aggregator settling values
      Pair<Integer, Integer> mean = mapForRow.get("ME");
      for (int idx = 0; idx < org.length; idx++)
      {
        String type = getAggCode(org[idx]);
        if (type != null) {
          Pair<Integer, Integer> aggRange = mapForRow.get(type);
          applied[idx] = new RangeAggregatorFactory(org[idx], aggRange.lhs, aggRange.rhs);
        } else if (mean != null) {
          applied[idx] = new RangeAggregatorFactory(org[idx], mean.lhs, mean.rhs);
        } else {
          applied[idx] = org[idx];
        }
      }
    } else {
      for (int idx = 0; idx < org.length; idx++)
      {
        applied[idx] = org[idx];
      }
    }
  }

  private String getAggCode(AggregatorFactory aggregatorFactory)
  {
    if (aggregatorFactory instanceof DoubleMinAggregatorFactory
        || aggregatorFactory instanceof LongMinAggregatorFactory) {
      return "MI";
    } else if (aggregatorFactory instanceof DoubleMaxAggregatorFactory
        || aggregatorFactory instanceof LongMaxAggregatorFactory) {
      return "MA";
    }

    return null;
  }

  private Map<String, Pair<Integer, Integer>> getValueMap(InputRow row)
  {
    int index = 0;
    for (String column: constColumns) {
      // it assumes that dimension value is not array
      dimValues[index++] = row.getDimension(column).get(0);
    }
    MultiKey key = new MultiKey(dimValues, false);
    Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> matcherMap = settlingMap.get(key);

    index = 0;
    for (String column: regexColumns) {
      // it assumes that dimension value is not array
      regexValues[index++] = row.getDimension(column).get(0);
    }

    for (Map.Entry<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> matcher: matcherMap.entrySet())
    {
      if (matcher.getKey().matches(regexValues)) {
        return matcher.getValue();
      }
    }

    return null;
  }

  private void fillMap()
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
      String[] keys = new String[constColumns.size()];
      String[] regexKeys = new String[regexColumns.size()];
      for (Map<String, Object> row: results)
      {
        int idx = 0;
        for (String column: constColumns)
        {
          keys[idx++] = (String)row.get(column);
        }
        MultiKey key = new MultiKey(keys);
        Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> settlingMatcherMap = settlingMap.get(key);
        if (settlingMatcherMap == null) {
          settlingMatcherMap = Maps.newHashMap();
          settlingMap.put(key, settlingMatcherMap);
        }

        idx = 0;
        for (String column: regexColumns)
        {
          regexKeys[idx++] = (String)row.get(column);
        }
        HadoopSettlingMatcher settlingMatcher = matcherFactory.getSettlingMatcher(regexKeys);
        Map<String, Pair<Integer, Integer>> map = settlingMatcherMap.get(settlingMatcher);
        if (map == null) {
          map = Maps.newHashMap();
          settlingMatcherMap.put(settlingMatcher, map);
        }

        String type = (String)row.get(aggTypeColumn);
        Pair<Integer, Integer> value =
            new Pair<>((int)Float.parseFloat((String)row.get(offsetColumn)), (int)Float.parseFloat((String)row.get(sizeColumn)));
        map.put(type, value);
      }

      handle.close();

      getSource = true;
    }
  }
}
