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

  public Map<String, Object> parse(String input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      JsonNode root = objectMapper.readTree(input);

      Iterator<String> keysIter = root.fieldNames();

      while (keysIter.hasNext()) {
        String key = keysIter.next();

        if (checkKeys.containsKey(key)) {
          JsonNode node = root.path(key);

          if (node.isArray()) {
            final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
            for (final JsonNode subNode : node) {
              final Object subNodeValue = convertAndApplyFilter(key, subNode);
              if (subNodeValue != null) {
                nodeValue.add(subNodeValue);
              }
            }
            if (nodeValue.size() > 0) {
              map.put(key, nodeValue);
            }
          } else {
            final Object nodeValue = convertAndApplyFilter(key, node);
            if (nodeValue != null) {
              map.put(key, nodeValue);
            }
          }
        }
      }
      return map;
    }
    catch (Exception e) {
      throw new RuntimeException(String.format("Unable to parse row [%s]", input), e);
    }
  }

  private Object convertAndApplyFilter(String key, JsonNode node)
  {
    final Object nodeValue = JSONParser.valueFunction.apply(node);
    if (nodeValue == null) {
      return null;
    }
    Predicate predicate = checkKeys.get(key);
    if (predicate == null) {
      return nodeValue;
    }

    return predicate.apply(nodeValue) ? nodeValue : null;
  }

}
