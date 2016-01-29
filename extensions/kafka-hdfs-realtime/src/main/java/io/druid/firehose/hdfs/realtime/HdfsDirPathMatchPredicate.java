package io.druid.firehose.hdfs.realtime;

import com.google.common.base.Predicate;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;


public class HdfsDirPathMatchPredicate implements Predicate<String> {
  final HashSet<String> dirPathSets;

  public HdfsDirPathMatchPredicate(
      String[] dirPaths
  )
  {
    dirPathSets = new HashSet<>(Arrays.asList(dirPaths));
  }

  @Override
  public boolean apply(String s) {
    File path = new File(s);

    return dirPathSets.contains(path.getParent());
  }
}
