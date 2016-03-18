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

package io.druid.firehose.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.collect.Utils;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.lang.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JDBCFirehoseFactory implements FirehoseFactory<MapInputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(JDBCFirehoseFactory.class);

  private final MetadataStorageConnectorConfig connectorConfig;
  private final String table;
  private final List<String> columns;

  @JsonCreator
  public JDBCFirehoseFactory(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true)
      final String table,
      @Nullable @JsonProperty(value = "columns", required = false)
      final List<String> columns
      )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    this.columns = columns;
  }

  @Override
  public Firehose connect(final MapInputRowParser parser) throws IOException, ParseException
  {
    final DBI dbi = new DBI(
        connectorConfig.getConnectURI(),
        connectorConfig.getUser(),
        connectorConfig.getPassword()
    );

    final List<String> requiredColumns;
    if (columns == null) {
      requiredColumns = null;
    } else {
      requiredColumns = columns;
    }

    verifyParserSpec(parser.getParseSpec());

    final Iterator<InputRow> rowIterator = dbi.withHandle(
        new HandleCallback<Iterator<InputRow>>()
        {
          @Override
          public Iterator<InputRow> withHandle(Handle handle) throws Exception
        {
            final String query = makeQuery(requiredColumns);

            return handle
                .createQuery(query)
                .map(
                    new ResultSetMapper<InputRow>()
                    {
                      @Override
                      public InputRow map(
                          final int index,
                          final ResultSet r,
                          final StatementContext ctx
                      ) throws SQLException
                      {
                        List<Object> values = Lists.newArrayListWithCapacity(requiredColumns.size());
                        for (String column: requiredColumns) {
                          values.add(r.getObject(column));
                        }

                        return parser.parse(convert(values, requiredColumns));
                      }
                    }
                ).iterator();
          }
        }
    );

    return new Firehose() {
      @Override
      public boolean hasMore()
      {
        return rowIterator.hasNext();
      }

      @Override
      public InputRow nextRow()
      {
        return rowIterator.next();
      }

      @Override
      public Runnable commit()
      {
        return null;
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  private void verifyParserSpec(ParseSpec parseSpec)
  {
    String tsColumn = parseSpec.getTimestampSpec().getTimestampColumn();
    Preconditions.checkArgument(columns.contains(tsColumn),
        String.format("timestamp column %s is not exists in table %s", tsColumn, table));

    for (String dim: parseSpec.getDimensionsSpec().getDimensions())
    {
      Preconditions.checkArgument(columns.contains(dim),
          String.format("dimension column %s is not exists in table %s", dim, table));
    }
  }

  private String makeQuery(List<String> requiredFields)
  {
    return new StringBuilder("SELECT ").append(StringUtils.join(requiredFields, ','))
                                       .append(" from ")
                                       .append(table)
                                       .toString();
  }

  private Map<String, Object> convert(List<Object> values, List<String> requiredFields)
  {
    Preconditions.checkArgument(values.size() == requiredFields.size());

    return Utils.zipMap(requiredFields, values);
  }
}
