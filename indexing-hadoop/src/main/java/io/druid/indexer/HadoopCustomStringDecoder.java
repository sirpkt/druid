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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DelimitedParseSpec;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

class HadoopCustomStringDecoder implements HadoopCustomDecoder
{
  static final Logger log = new Logger(HadoopCustomStringDecoder.class);

  final Map<String, String> parseColumn;
  private List<String> dimensionList;
  private List<String> columnList;
  private List<String> metricList;



  @JsonCreator
  public HadoopCustomStringDecoder(
      @JsonProperty("parseColumn") Map<String, String> parseColumn
  )
  {
    if (parseColumn == null) {
      parseColumn = Maps.newHashMap();
    }
    this.parseColumn = parseColumn;

  }

  @JsonProperty
  public Map<String, String> getParseColumn()
  {
    return parseColumn;
  }

  @Override
  public InputRow decodeMessage(String msg, DelimitedParseSpec inputParseSpec)
  {


    final Map<String, String> dimensions = Maps.newHashMap();
    final Map<String, Float> metrics = Maps.newHashMap();
    final Map<String, String> columns = Maps.newHashMap();
    dimensionList = inputParseSpec.getDimensionsSpec().getDimensions();
    columnList = inputParseSpec.getColumns();
    final String timeField = inputParseSpec.getTimestampSpec().getTimestampColumn();


//TODO::: dimension, columns put

    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dimensionList;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        DateTime timestamp = (DateTime)getRaw(timeField);
        return timestamp.getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return (DateTime)getRaw(timeField);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        final String value = dimensions.get(dimension);
        if (value != null) {
          return ImmutableList.of(value);
        } else {
          return ImmutableList.of();
        }
      }

      @Override
      public Object getRaw(String dimension)
      {
        return dimensions.get(dimension);
      }


      @Override
      public float getFloatMetric(String metric)
      {
        return metrics.get(metric);
      }

      @Override
      public long getLongMetric(String metric)
      {
        return new Float(metrics.get(metric)).longValue();
      }

      @Override
      public int compareTo(Row o)
      {
        DateTime timestamp = (DateTime)getRaw(timeField);
        return timestamp.compareTo(o.getTimestamp());
      }

      @Override
      public String toString()
      {
        return "HadoopCustomStringDecoderRow{" +
               "columns=" + columns +
               ", dimensions=" + dimensions +
               ", metrics=" + metrics +
               '}';
      }
    };
  }
}
