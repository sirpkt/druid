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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 *
 */
@JsonTypeName("jdbc")
public class JDBCExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final String keyColumn;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final String query;
  @JsonProperty
  private final String tsColumn;
  @JsonProperty
  private final String namespace;
  @JsonProperty
  private final Period pollPeriod;

  @JsonCreator
  public JDBCExtractionNamespace(
      @NotNull @JsonProperty(value = "namespace", required = true)
      final String namespace,
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @JsonProperty(value = "table", required = true)
      final String table,
      @JsonProperty(value = "keyColumn", required = false)
      final String keyColumn,
      @JsonProperty(value = "valueColumn", required = false)
      final String valueColumn,
      @JsonProperty(value = "query", required = false)
      final String query,
      @Nullable @JsonProperty(value = "tsColumn", required = false)
      final String tsColumn,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
      final Period pollPeriod
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    Preconditions.checkArgument((keyColumn != null && valueColumn != null)||(query != null),
        "(keyColumn, valueColumn) or query should be specified");
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.query = query;
    this.tsColumn = tsColumn;
    this.namespace = Preconditions.checkNotNull(namespace, "namespace");
    this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  public String getTable()
  {
    return table;
  }

  public String getKeyColumn()
  {
    return keyColumn;
  }

  public String getValueColumn()
  {
    return valueColumn;
  }

  public String getQuery()
  {
    return query;
  }

  public String getTsColumn()
  {
    return tsColumn;
  }

  @Override
  public long getPollMs()
  {
    return pollPeriod.toStandardDuration().getMillis();
  }

  @Override
  public String toString()
  {
    if (query != null) {
      return String.format(
          "JDBCExtractionNamespace = { namespace = %s, connectorConfig = { %s }, query = %s, tsColumn = %s, pollPeriod = %s}",
          namespace,
          connectorConfig.toString(),
          query,
          tsColumn,
          pollPeriod
      );
    } else {
      return String.format(
          "JDBCExtractionNamespace = { namespace = %s, connectorConfig = { %s }, table = %s, keyColumn = %s, valueColumn = %s, tsColumn = %s, pollPeriod = %s}",
          namespace,
          connectorConfig.toString(),
          table,
          keyColumn,
          valueColumn,
          tsColumn,
          pollPeriod
      );
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JDBCExtractionNamespace that = (JDBCExtractionNamespace) o;

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (!keyColumn.equals(that.keyColumn)) {
      return false;
    }
    if (!valueColumn.equals(that.valueColumn)) {
      return false;
    }
    if (!query.equals(that.query)) {
      return false;
    }
    if (tsColumn != null ? !tsColumn.equals(that.tsColumn) : that.tsColumn != null) {
      return false;
    }
    if (!namespace.equals(that.namespace)) {
      return false;
    }
    return pollPeriod.equals(that.pollPeriod);

  }

  @Override
  public int hashCode()
  {
    int result = connectorConfig.hashCode();
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + (keyColumn != null ? keyColumn.hashCode() : 0);
    result = 31 * result + (valueColumn != null ? valueColumn.hashCode() : 0);
    result = 31 * result + (query != null ? query.hashCode() : 0);
    result = 31 * result + (tsColumn != null ? tsColumn.hashCode() : 0);
    result = 31 * result + namespace.hashCode();
    result = 31 * result + pollPeriod.hashCode();
    return result;
  }
}
