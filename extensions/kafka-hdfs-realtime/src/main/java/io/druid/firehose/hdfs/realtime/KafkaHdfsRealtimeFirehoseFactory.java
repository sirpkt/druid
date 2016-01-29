package io.druid.firehose.hdfs.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 */
public class KafkaHdfsRealtimeFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = Logger.getLogger(KafkaHdfsRealtimeFirehoseFactory.class);

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final Map<String, Object> hdfsPathSpec;

  private final String[] hdfsPaths;

  private final KafkaTrumpetEventParser eventParser;
  private final Map<String, Predicate> eventFilterSpec;

  @JsonProperty
  private final String kafkaEventFeed;

  @JsonProperty
  private final String[] checkingEvents;

  @JsonCreator
  public KafkaHdfsRealtimeFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("hdfsInputSpec") Map<String, Object> hdfsPathSpec,
      @JsonProperty("kafkaEventFeed") String kafkaEventFeed,
      @JsonProperty("checkingEvents") String[] checkingEvents
  )
  {
    this.consumerProps = consumerProps;
    this.hdfsPathSpec = hdfsPathSpec;
    this.kafkaEventFeed = kafkaEventFeed;
    this.checkingEvents = checkingEvents;

    this.hdfsPaths = new String[hdfsPathSpec.size()];
    int hdfsPathIdx = 0;

    for(Map.Entry<String, Object> entry: hdfsPathSpec.entrySet()) {
      Preconditions.checkArgument("static".equals(entry.getKey()),
          "[%s]=> only \"static\" is supported", entry.getKey());
      this.hdfsPaths[hdfsPathIdx++] = (String)entry.getValue();
    }
    this.eventFilterSpec = new HashMap<>();
    for (String checkingEvent: checkingEvents) {
      this.eventFilterSpec.put(checkingEvent, null);
    }
    // Basically, "path" event filter is included for the given HDFS input paths
    this.eventFilterSpec.put("path", new HdfsDirPathMatchPredicate(this.hdfsPaths));
    this.eventParser = new KafkaTrumpetEventParser(this.eventFilterSpec);

    // TODO - fix to make kafka consumer group id unique
    //        currently collision may occur for concurrent creation of factory
    this.consumerProps.setProperty("group.id", this.consumerProps.getProperty("group.id") + System.currentTimeMillis());
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser) throws IOException
  {
    Set<String> newDimExclus = Sets.union(
        firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
        Sets.newHashSet("kafkaEventFeed")
    );
    final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
        firehoseParser.getParseSpec()
            .withDimensionsSpec(
                firehoseParser.getParseSpec()
                    .getDimensionsSpec()
                    .withDimensionExclusions(
                        newDimExclus
                    )
            )
    );

    final ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

    final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(
        ImmutableMap.of(
            kafkaEventFeed,
            1
        )
    );

    final List<KafkaStream<byte[], byte[]>> streamList = streams.get(kafkaEventFeed);
    if (streamList == null || streamList.size() != 1) {
      return null;
    }

    final KafkaStream<byte[], byte[]> stream = streamList.get(0);
    final ConsumerIterator<byte[], byte[]> iter = stream.iterator();

    return new Firehose()
    {
      InputRow next = null;

      @Override
      public boolean hasMore()
      {
        while (iter.hasNext()) {

        }
      }

      private boolean getAndCheckNextRow()
      {
        final byte[] message = iter.next().message();

        if (message == null) {
          return false;
        }

        return theParser.parse(ByteBuffer.wrap(message));
      }

      @Override
      public InputRow nextRow()
      {
        final byte[] message = iter.next().message();

        if (message == null) {
          return null;
        }

        return theParser.parse(ByteBuffer.wrap(message));
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
            log.info("committing offsets");
            connector.commitOffsets();
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        connector.shutdown();
      }
    };
  }

}