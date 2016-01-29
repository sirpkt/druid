package io.druid.firehose.hdfs.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.JSONParser;

import java.util.*;

public class KafkaTrumpetEventParser {
  private final ObjectMapper objectMapper;
  private final Map<String, Predicate> checkKeys;

  public KafkaTrumpetEventParser(
      Map<String, Predicate> checkKeys
  )
  {
    this.objectMapper = new ObjectMapper();
    this.checkKeys = checkKeys;
  }

  public Map<String, Object> parse(Map<String, Object> input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();

      for (Map.Entry<String, Object> entry: input.entrySet()) {
        final String key = entry.getKey();
        if (checkKeys.containsKey(key)) {
          final Object value = entry.getValue();
          if (value instanceof List) {
            List<Object> valueList = (List) value;
            final List<Object> arrayValues = Lists.newArrayListWithExpectedSize(valueList.size());
            for (Object arrayEntry: valueList) {
              final Object filtered = convertAndApplyFilter(key, arrayEntry);
              if (filtered != null) {
                arrayValues.add(filtered);
              }
            }
            if (valueList.size() > 0) {
              map.put(key, valueList);
            }
          } else {
            final Object filtered = convertAndApplyFilter(key, entry.getValue());
            if (filtered != null) {
              map.put(key, filtered);
            }
          }
        }
      }
      return map.size() > 0 ? map: null;
    }
    catch (Exception e) {
      throw new RuntimeException(String.format("Unable to parse row [%s]", input), e);
    }
  }

  private Object convertAndApplyFilter(String key, Object value)
  {
    if (value == null) {
      return null;
    }
    Predicate predicate = checkKeys.get(key);
    if (predicate == null) {
      return value;
    }

    return predicate.apply(value) ? value : null;
  }

}
