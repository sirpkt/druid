package io.druid.firehose.hdfs.realtime;

import com.google.common.base.Predicate;

import java.util.Arrays;
import java.util.HashSet;

public class KafkaTrumpetEventTypePredicate implements Predicate<String> {
  final HashSet<String> dirPathSets;

  public KafkaTrumpetEventTypePredicate(
      String[] eventTypes
  )
  {
    dirPathSets = new HashSet<>(Arrays.asList(eventTypes));
  }

  @Override
  public boolean apply(String type) {
    return dirPathSets.contains(type);
  }
}
