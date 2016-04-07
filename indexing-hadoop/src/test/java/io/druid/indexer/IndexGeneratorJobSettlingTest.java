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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class IndexGeneratorJobSettlingTest
{
  private String hiveConnectorMeta = "{\"createTables\":true,\"connectURI\":\"jdbc:hive2://emn-g04-03:10000\",\"user\":\"hadoop\",\"password\":\"hadoop\"}";
  private List<String> staticColumns = Arrays.asList("module_name", "eqp_param_name");
  private List<String> regexColumns = Arrays.asList("eqp_recipe_id", "eqp_step_id", "lot_code");
  private String aggTypeColumn = "sum_type_cd";
  private String offsetColumn = "count_settling";
  private String sizeColumn = "count_activation";
  private String targetTable = "big_fdc_settling_info";
  private String condition = "count_settling > 0.0 and count_activation != -1.0";

  final private static AggregatorFactory[] aggs1 = {
      new LongSumAggregatorFactory("visited_num", "visited_num")
  };

  @Parameterized.Parameters(name = "partitionType={0}, interval={1}, shardInfoForEachSegment={2}, " +
      "data={3}, inputFormatName={4}, inputRowParser={5}, maxRowsInMemory={6}, " +
      "aggs={7}, datasourceName={8}")
  public static Collection<Object[]> constructFeed()
  {
    final List<Object[]> baseConstructors = Arrays.asList(
        new Object[][]{
            {
                "hashed",
                "2014-10-22T00:00:00Z/P1D",
                new Integer[][][]{
                    {
                        {0, 4},
                        {1, 4},
                        {2, 4},
                        {3, 4}
                    }
                },
                ImmutableList.of(
                    "2014102200,a.example.com,100",
                    "2014102201,b.exmaple.com,50",
                    "2014102202,c.example.com,200",
                    "2014102203,d.example.com,250",
                    "2014102204,e.example.com,123",
                    "2014102205,f.example.com,567",
                    "2014102206,g.example.com,11",
                    "2014102207,h.example.com,251",
                    "2014102208,i.example.com,963",
                    "2014102209,j.example.com,333",
                    "2014102210,k.example.com,253",
                    "2014102211,l.example.com,321",
                    "2014102212,m.example.com,3125",
                    "2014102213,n.example.com,234",
                    "2014102214,o.example.com,325",
                    "2014102215,p.example.com,3533",
                    "2014102216,q.example.com,500",
                    "2014102216,q.example.com,87"
                ),
                null,
                new HadoopyStringInputRowParser(
                    new CSVParseSpec(
                        new TimestampSpec("timestamp", "yyyyMMddHH", null),
                        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                        null,
                        ImmutableList.of("timestamp", "host", "visited_num")
                    )
                ),
                null,
                aggs1,
                "website"
            }
        }
    );

    return baseConstructors;
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final String partitionType;
  private final Interval interval;
  private final Object[][][] shardInfoForEachSegment;
  private final List<String> data;
  private final String inputFormatName;
  private final InputRowParser inputRowParser;
  private final Integer maxRowsInMemory;
  private final AggregatorFactory[] aggs;
  private final String datasourceName;

  private ObjectMapper mapper;
  private HadoopDruidIndexerConfig config;
  private File dataFile;
  private File tmpDir;

  public IndexGeneratorJobSettlingTest(
      String partitionType,
      String interval,
      Object[][][] shardInfoForEachSegment,
      List<String> data,
      String inputFormatName,
      InputRowParser inputRowParser,
      Integer maxRowsInMemory,
      AggregatorFactory[] aggs,
      String datasourceName
  ) throws IOException
  {
    this.partitionType = partitionType;
    this.shardInfoForEachSegment = shardInfoForEachSegment;
    this.interval = new Interval(interval);
    this.data = data;
    this.inputFormatName = inputFormatName;
    this.inputRowParser = inputRowParser;
    this.maxRowsInMemory = maxRowsInMemory;
    this.aggs = aggs;
    this.datasourceName = datasourceName;
  }

  private void writeDataToLocalSequenceFile(File outputFile, List<String> data) throws IOException
  {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Writer fileWriter = SequenceFile.createWriter(
        fs,
        conf,
        new Path(outputFile.getAbsolutePath()),
        BytesWritable.class,
        BytesWritable.class,
        SequenceFile.CompressionType.NONE,
        (CompressionCodec) null
    );

    int keyCount = 10;
    for (String line : data) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      buf.putInt(keyCount);
      BytesWritable key = new BytesWritable(buf.array());
      BytesWritable value = new BytesWritable(line.getBytes(Charsets.UTF_8));
      fileWriter.append(key, value);
      keyCount += 1;
    }

    fileWriter.close();
  }

  @Before
  public void setUp() throws Exception
  {
    mapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    mapper.registerSubtypes(new NamedType(SingleDimensionShardSpec.class, "single"));

    dataFile = temporaryFolder.newFile();
    tmpDir = temporaryFolder.newFolder();

    HashMap<String, Object> inputSpec = new HashMap<String, Object>();
    inputSpec.put("paths", dataFile.getCanonicalPath());
    inputSpec.put("type", "static");
    if (inputFormatName != null) {
      inputSpec.put("inputFormat", inputFormatName);
    }

    if (SequenceFileInputFormat.class.getName().equals(inputFormatName)) {
      writeDataToLocalSequenceFile(dataFile, data);
    } else {
      FileUtils.writeLines(dataFile, data);
    }

    List<String> columns = ListUtils.union(staticColumns, regexColumns);
    columns.add(aggTypeColumn);
    columns.add(offsetColumn);
    columns.add(sizeColumn);

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                datasourceName,
                mapper.convertValue(
                    inputRowParser,
                    Map.class
                ),
                aggs,
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularity.NONE, ImmutableList.of(this.interval)
                ),
                mapper
            ),
            new HadoopIOConfig(
                ImmutableMap.copyOf(inputSpec),
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
                null,
                null,
                maxRowsInMemory,
                false,
                false,
                false,
                false,
                ImmutableMap.of(JobContext.NUM_REDUCES, "0"), //verifies that set num reducers is ignored
                false,
                false,
                null,
                true,
                null
            ),
            null,
            new HadoopSettlingConfig(
                mapper.readValue(hiveConnectorMeta, MetadataStorageConnectorConfig.class),
                String.format("select %s from %s where %s", StringUtils.join(columns, ","), targetTable, condition),
                staticColumns,
                regexColumns,
                aggTypeColumn,
                offsetColumn,
                sizeColumn
            )
        )
    );

    config.setShardSpecs(loadShardSpecs(partitionType, shardInfoForEachSegment));
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
  }

  private List<ShardSpec> constructShardSpecFromShardInfo(String partitionType, Object[][] shardInfoForEachShard)
  {
    List<ShardSpec> specs = Lists.newArrayList();
    if (partitionType.equals("hashed")) {
      for (Integer[] shardInfo : (Integer[][]) shardInfoForEachShard) {
        specs.add(new HashBasedNumberedShardSpec(shardInfo[0], shardInfo[1], null, HadoopDruidIndexerConfig.JSON_MAPPER));
      }
    } else if (partitionType.equals("single")) {
      int partitionNum = 0;
      for (String[] shardInfo : (String[][]) shardInfoForEachShard) {
        specs.add(new SingleDimensionShardSpec("host", shardInfo[0], shardInfo[1], partitionNum++));
      }
    } else {
      throw new RuntimeException(String.format("Invalid partition type:[%s]", partitionType));
    }

    return specs;
  }

  private Map<DateTime, List<HadoopyShardSpec>> loadShardSpecs(
      String partitionType,
      Object[][][] shardInfoForEachShard
  )
  {
    Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
    int shardCount = 0;
    int segmentNum = 0;
    for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
      List<ShardSpec> specs = constructShardSpecFromShardInfo(partitionType, shardInfoForEachShard[segmentNum++]);
      List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
      for (int i = 0; i < specs.size(); ++i) {
        actualSpecs.add(new HadoopyShardSpec(specs.get(i), shardCount++));
      }

      shardSpecs.put(segmentGranularity.getStart(), actualSpecs);
    }

    return shardSpecs;
  }

  @Test
  public void testIndexGeneratorJob() throws IOException
  {
    verifyJob(new IndexGeneratorJob(config));
  }

  private void verifyJob(IndexGeneratorJob job) throws IOException
  {
    JobHelper.runJobs(ImmutableList.<Jobby>of(job), config);

    int segmentNum = 0;
    for (DateTime currTime = interval.getStart(); currTime.isBefore(interval.getEnd()); currTime = currTime.plusDays(1)) {
      Object[][] shardInfo = shardInfoForEachSegment[segmentNum++];
      File segmentOutputFolder = new File(
          String.format(
              "%s/%s/%s_%s/%s",
              config.getSchema().getIOConfig().getSegmentOutputPath(),
              config.getSchema().getDataSchema().getDataSource(),
              currTime.toString(),
              currTime.plusDays(1).toString(),
              config.getSchema().getTuningConfig().getVersion()
          )
      );
      Assert.assertTrue(segmentOutputFolder.exists());
      Assert.assertEquals(shardInfo.length, segmentOutputFolder.list().length);

      for (int partitionNum = 0; partitionNum < shardInfo.length; ++partitionNum) {
        File individualSegmentFolder = new File(segmentOutputFolder, Integer.toString(partitionNum));
        Assert.assertTrue(individualSegmentFolder.exists());

        File descriptor = new File(individualSegmentFolder, "descriptor.json");
        File indexZip = new File(individualSegmentFolder, "index.zip");
        Assert.assertTrue(descriptor.exists());
        Assert.assertTrue(indexZip.exists());

        DataSegment dataSegment = mapper.readValue(descriptor, DataSegment.class);
        Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
        Assert.assertEquals(new Interval(currTime, currTime.plusDays(1)), dataSegment.getInterval());
        Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
        Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
        Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

        if (datasourceName.equals("website")) {
          Assert.assertEquals("website", dataSegment.getDataSource());
          Assert.assertEquals("host", dataSegment.getDimensions().get(0));
          Assert.assertEquals("visited_num", dataSegment.getMetrics().get(0));
        } else if (datasourceName.equals("inherit_dims")) {
          Assert.assertEquals("inherit_dims", dataSegment.getDataSource());
          Assert.assertEquals(ImmutableList.of("X", "Y", "M", "Q", "B", "F"), dataSegment.getDimensions());
          Assert.assertEquals("count", dataSegment.getMetrics().get(0));
        } else if (datasourceName.equals("inherit_dims2")) {
          Assert.assertEquals("inherit_dims2", dataSegment.getDataSource());
          Assert.assertEquals(ImmutableList.of("B", "F", "M", "Q", "X", "Y"), dataSegment.getDimensions());
          Assert.assertEquals("count", dataSegment.getMetrics().get(0));
        } else {
          Assert.fail("Test did not specify supported datasource name");
        }

        if (partitionType.equals("hashed")) {
          Integer[] hashShardInfo = (Integer[]) shardInfo[partitionNum];
          HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
          Assert.assertEquals((int) hashShardInfo[0], spec.getPartitionNum());
          Assert.assertEquals((int) hashShardInfo[1], spec.getPartitions());
        } else if (partitionType.equals("single")) {
          String[] singleDimensionShardInfo = (String[]) shardInfo[partitionNum];
          SingleDimensionShardSpec spec = (SingleDimensionShardSpec) dataSegment.getShardSpec();
          Assert.assertEquals(singleDimensionShardInfo[0], spec.getStart());
          Assert.assertEquals(singleDimensionShardInfo[1], spec.getEnd());
        } else {
          throw new RuntimeException(String.format("Invalid partition type:[%s]", partitionType));
        }
      }
    }
  }

}
