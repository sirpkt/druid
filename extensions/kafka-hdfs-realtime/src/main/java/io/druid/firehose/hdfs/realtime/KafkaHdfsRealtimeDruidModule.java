package io.druid.firehose.hdfs.realtime;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 */
public class KafkaHdfsRealtimeDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("KafkaHdfsRealtimeFirehoseModule")
            .registerSubtypes(
                new NamedType(KafkaHdfsRealtimeFirehoseFactory.class, "hdfs-realtime")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
