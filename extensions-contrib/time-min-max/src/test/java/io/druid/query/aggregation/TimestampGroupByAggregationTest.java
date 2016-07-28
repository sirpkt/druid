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

package io.druid.query.aggregation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.List;

@RunWith(Parameterized.class)
public class TimestampGroupByAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private Timestamp[] values = new Timestamp[10];

  @Parameterized.Parameters(name="{index}: Test for {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        ImmutableList.of(
            ImmutableList.of("timeMin", "tmin", "time_min", TimestampMinAggregatorFactory.class, DateTime.parse("2011-01-12T01:00:00.000Z")),
            ImmutableList.of("timeMax", "tmax", "time_max", TimestampMaxAggregatorFactory.class, DateTime.parse("2011-01-31T01:00:00.000Z"))
        ),
        new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private String aggType;
  private String aggField;
  private String groupByField;
  private Class<? extends TimestampAggregatorFactory> aggClass;
  private DateTime expected;

  public TimestampGroupByAggregationTest(String aggType, String aggField, String groupByField, Class<? extends TimestampAggregatorFactory> aggClass, DateTime expected)
  {
    this.aggType = aggType;
    this.aggField = aggField;
    this.groupByField = groupByField;
    this.aggClass = aggClass;
    this.expected = expected;
  }

  @Before
  public void setup() throws Exception
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new TimestampMinMaxModule().getJacksonModules(),
        temporaryFolder
    );

    selector = new TestObjectColumnSelector(values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeObjectColumnSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);

  }

  @Test
  public void testSimpleDataIngestionAndGroupByTest() throws Exception
  {
    String recordParser = "{\n" +
        "  \"type\": \"string\",\n" +
        "  \"parseSpec\": {\n" +
        "    \"format\": \"tsv\",\n" +
        "    \"timestampSpec\": {\n" +
        "      \"column\": \"timestamp\",\n" +
        "      \"format\": \"auto\"\n" +
        "    },\n" +
        "    \"dimensionsSpec\": {\n" +
        "      \"dimensions\": [\n" +
        "        \"product\"\n" +
        "      ],\n" +
        "      \"dimensionExclusions\": [],\n" +
        "      \"spatialDimensions\": []\n" +
        "    },\n" +
        "    \"columns\": [\n" +
        "      \"timestamp\",\n" +
        "      \"cat\",\n" +
        "      \"product\",\n" +
        "      \"prefer\",\n" +
        "      \"prefer2\",\n" +
        "      \"pty_country\"\n" +
        "    ]\n" +
        "  }\n" +
        "}";
    String aggregator = "[\n" +
        "  {\n" +
        "    \"type\": \"" + aggType + "\",\n" +
        "    \"name\": \"" + aggField + "\",\n" +
        "    \"fieldName\": \"timestamp\"\n" +
        "  }\n" +
        "]";
    String groupBy = "{\n" +
        "  \"queryType\": \"groupBy\",\n" +
        "  \"dataSource\": \"test_datasource\",\n" +
        "  \"granularity\": \"MONTH\",\n" +
        "  \"dimensions\": [\"product\"],\n" +
        "  \"aggregations\": [\n" +
        "    {\n" +
        "      \"type\": \"" + aggType + "\",\n" +
        "      \"name\": \"" + groupByField + "\",\n" +
        "      \"fieldName\": \"" + aggField + "\"\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [\n" +
        "    \"2011-01-01T00:00:00.000Z/2011-05-01T00:00:00.000Z\"\n" +
        "  ]\n" +
        "}";
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("druid.sample.tsv"),
        recordParser,
        aggregator,
        0,
        QueryGranularities.MONTH,
        100,
        groupBy
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(36, results.size());
    Assert.assertEquals(expected, ((MapBasedRow)results.get(0)).getEvent().get(groupByField));
  }
}
