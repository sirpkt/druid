/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package io.druid.server.lookup.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.server.lookup.PrefetchableFetcher;
import org.apache.commons.collections4.CollectionUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class JdbcDataFetcher extends PrefetchableFetcher<String, String>
{
  private static final Logger LOGGER = new Logger(JdbcDataFetcher.class);
  private static final int DEFAULT_STREAMING_FETCH_SIZE = 1000;

  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final String keyColumn;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final int streamingFetchSize;
  @JsonProperty
  private final List<String> prefetchRanges;

  private final String fetchAllQuery;
  private final String fetchQuery;
  private final String prefetchQueryFromTo;
  private final String prefetchQueryFromOnly;
  private final String prefetchQueryToOnly;
  private final String reverseFetchQuery;
  private final DBI dbi;
  private final NavigableSet<String> sortedRanges;

  public JdbcDataFetcher(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig,
      @JsonProperty("table") String table,
      @JsonProperty("keyColumn") String keyColumn,
      @JsonProperty("valueColumn") String valueColumn,
      @JsonProperty("streamingFetchSize") Integer streamingFetchSize,
      @JsonProperty("prefetchRanges") List<String> rangeKeys
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    this.streamingFetchSize = streamingFetchSize == null ? DEFAULT_STREAMING_FETCH_SIZE : streamingFetchSize;
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = Preconditions.checkNotNull(keyColumn, "keyColumn");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");
    this.prefetchRanges = rangeKeys;
    this.sortedRanges = new TreeSet<>(this.prefetchRanges);

    this.fetchAllQuery = String.format(
        "SELECT %s, %s FROM %s",
        this.keyColumn,
        this.valueColumn,
        this.table
    );
    this.fetchQuery = String.format(
        "SELECT %s FROM %s WHERE %s = :val",
        this.valueColumn,
        this.table,
        this.keyColumn
    );
    this.prefetchQueryFromTo = String.format(
        "SELECT %s, %s FROM %s WHERE '%%s' <= %s AND %s < '%%s'",
        this.keyColumn,
        this.valueColumn,
        this.table,
        this.keyColumn,
        this.keyColumn
    );
    this.prefetchQueryFromOnly = String.format(
        "SELECT %s, %s FROM %s WHERE %s < '%%s'",
        this.keyColumn,
        this.valueColumn,
        this.table,
        this.keyColumn
    );
    this.prefetchQueryToOnly = String.format(
        "SELECT %s, %s FROM %s WHERE '%%s' <= %s",
        this.keyColumn,
        this.valueColumn,
        this.table,
        this.keyColumn
    );
    this.reverseFetchQuery = String.format(
        "SELECT %s FROM %s WHERE %s = :val",
        this.keyColumn,
        this.table,
        this.valueColumn
    );
    dbi = new DBI(
        connectorConfig.getConnectURI(),
        connectorConfig.getUser(),
        connectorConfig.getPassword()
    );
    dbi.registerMapper(new KeyValueResultSetMapper(keyColumn, valueColumn));
  }

  @Override
  public Iterable<Map.Entry<String, String>> fetchAll()
  {
    return inReadOnlyTransaction(new TransactionCallback<List<Map.Entry<String, String>>>()
                                 {
                                   @Override
                                   public List<Map.Entry<String, String>> inTransaction(
                                       Handle handle,
                                       TransactionStatus status
                                   ) throws Exception
                                   {
                                     return handle.createQuery(fetchAllQuery)
                                         .setFetchSize(streamingFetchSize)
                                         .map(new KeyValueResultSetMapper(keyColumn, valueColumn))
                                         .list();
                                   }

                                 }
    );
  }

  @Override
  public String fetch(final String key)
  {
    List<String> pairs = inReadOnlyTransaction(
        new TransactionCallback<List<String>>()
        {
          @Override
          public List<String> inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            return handle.createQuery(fetchQuery)
                .bind("val", key)
                .map(StringMapper.FIRST)
                .list();
          }
        }
    );
    if (pairs.isEmpty()) {
      return null;
    }
    return Strings.nullToEmpty(pairs.get(0));
  }

  @Override
  public Iterable<Map.Entry<String, String>> fetch(final Iterable<String> keys)
  {
    QueryKeys queryKeys = dbi.onDemand(QueryKeys.class);
    return queryKeys.findNamesForIds(Lists.newArrayList(keys), table, keyColumn, valueColumn);
  }

  @Override
  public List<String> reverseFetchKeys(final String value)
  {
    List<String> results = inReadOnlyTransaction(new TransactionCallback<List<String>>()
    {
      @Override
      public List<String> inTransaction(Handle handle, TransactionStatus status) throws Exception
      {
        return handle.createQuery(reverseFetchQuery)
            .bind("val", value)
            .map(StringMapper.FIRST)
            .list();
      }
    });
    return results;
  }

  @Override
  public Map<String, String> prefetch(String key)
  {
    if (prefetchRanges == null) {
      return ImmutableMap.of();
    }

    final Pair<String, String> range = getRange(key);
    final String queryString =
        (range.lhs == null) ? String.format(prefetchQueryToOnly, range.rhs) :
            (range.rhs == null) ? String.format(prefetchQueryFromOnly, range.lhs)
                : String.format(prefetchQueryFromTo, range.lhs, range.rhs);

    return inReadOnlyTransaction(
        new TransactionCallback<Map<String, String>>()
        {
          @Override
          public Map<String, String> inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            List<Map<String, Object>> rowList = handle.createQuery(queryString)
                .setFetchSize(streamingFetchSize)
                .list();
            Map<String, String> rowMap = Maps.newHashMap();
            for (Map<String, Object> row: rowList) {
              rowMap.put((String) row.get(keyColumn), (String) row.get(valueColumn));
            }
            return rowMap;
          }
        }
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcDataFetcher)) {
      return false;
    }

    JdbcDataFetcher that = (JdbcDataFetcher) o;

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (!keyColumn.equals(that.keyColumn)) {
      return false;
    }
    if (!CollectionUtils.isEqualCollection(prefetchRanges, that.prefetchRanges)) {
      return false;
    }
    return valueColumn.equals(that.valueColumn);

  }

  private Pair<String, String> getRange(String key)
  {
    String start = sortedRanges.floor(key);
    String end = sortedRanges.higher(key);

    Preconditions.checkArgument(start != null || end != null, "No matching range");

    return new Pair<String, String>(start, end);
  }

  private DBI getDbi()
  {
    return dbi;
  }

  private <T> T inReadOnlyTransaction(final TransactionCallback<T> callback)
  {
    return getDbi().withHandle(
        new HandleCallback<T>()
        {
          @Override
          public T withHandle(Handle handle) throws Exception
          {
            final Connection connection = handle.getConnection();
            final boolean readOnly = connection.isReadOnly();
            connection.setReadOnly(true);
            try {
              return handle.inTransaction(callback);
            }
            finally {
              try {
                connection.setReadOnly(readOnly);
              }
              catch (SQLException e) {
                // at least try to log it so we don't swallow exceptions
                LOGGER.error(e, "Unable to reset connection read-only state");
              }
            }
          }
        }
    );
  }
}
