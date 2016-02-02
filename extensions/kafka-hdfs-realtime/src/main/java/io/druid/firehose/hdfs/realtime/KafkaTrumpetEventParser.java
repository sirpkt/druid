package io.druid.firehose.hdfs.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.metamx.common.parsers.JSONParser;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.*;

public class KafkaTrumpetEventParser {
  private final ObjectMapper objectMapper;
  private final Map<Map<String, Predicate>, List<String>> rule;
  private final HashSet<String> filteredFields;
  private final Charset charset = Charsets.UTF_8;
  private CharBuffer chars;

  public KafkaTrumpetEventParser(
      Map<Map<String, Predicate>, List<String>> rule
  )
  {
    this.objectMapper = new ObjectMapper();
    this.rule = rule;
    this.filteredFields = new HashSet<>();
    for (Map.Entry<Map<String, Predicate>, List<String>> entry: rule.entrySet()) {
      this.filteredFields.addAll(entry.getKey().keySet());
      this.filteredFields.addAll(entry.getValue());
    }
  }

  public KafkaTrumpetEventParser(
      Map<String, Predicate> condition,
      List<String> outputFields
  )
  {
    this.objectMapper = new ObjectMapper();
    this.rule = new HashMap<>();
    this.rule.put(condition, outputFields);
    this.filteredFields = new HashSet<>();
      this.filteredFields.addAll(condition.keySet());
      this.filteredFields.addAll(outputFields);
  }

  public Map<String, Object> parse(ByteBuffer input)
  {
    int payloadSize = input.remaining();
    if(this.chars == null || this.chars.remaining() < payloadSize) {
      this.chars = CharBuffer.allocate(payloadSize);
    }

    CoderResult coderResult = this.charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE).decode(input, this.chars, true);
    if(coderResult.isUnderflow()) {
      this.chars.flip();

      Map theMap;
      try {
        theMap = this.parse(this.chars.toString());
      } finally {
        this.chars.clear();
      }

      return theMap;
    } else {
      throw new RuntimeException(String.format("Failed with CoderResult[%s]", new Object[]{coderResult}));
    }
  }

  public Map<String, Object> parse(String input)
  {
    try {
      final Map<String, Object> map = new LinkedHashMap<>();

      JsonNode root = objectMapper.readTree(input);
      Iterator<String> keysIter = root.fieldNames();

      while (keysIter.hasNext()) {
        String key = keysIter.next();

        if (filteredFields.contains(key)) {
          JsonNode node = root.path(key);

          // currently, skip nested structured fields
          // SHOULD be added if needed
          if (!node.isArray()) {
            final Object nodeValue = convertValue(node);
            map.put(key, nodeValue);
          }
          /* 
          else {
            final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
            for (final JsonNode subNode : node) {
              final Object subNodeValue = convertValue(subNode);
              if (subNodeValue != null) {
                nodeValue.add(subNodeValue);
              }
            }
            map.put(key, nodeValue);
          } 
          */
        }
      }

      if (map.size() == 0) return null;

      // check filter conditions - if one of the condition passes, it will return the associated output fields
      for (Map.Entry<Map<String, Predicate>, List<String>> entry: rule.entrySet()) {
        boolean pass = true;
        for (Map.Entry<String, Predicate> condition: entry.getKey().entrySet()) {
          Object value = map.get(condition.getKey());
          if (value == null || !condition.getValue().apply(value)) {
            pass = false;
            break;
          }
        }
        if (pass) 
        {
          return Maps.toMap(entry.getValue(), new Function<String, Object>() {
            public Object apply(String key) {
              return map.get(key);
            }
          });
        }
      }

      return null;
    }
    catch (Exception e) {
      throw new RuntimeException(String.format("Unable to parse row [%s]", input), e);
    }
  }

  private Object convertValue(JsonNode node)
  {
    final Object nodeValue = JSONParser.valueFunction.apply(node);
    if (nodeValue == null) {
      return null;
    }
    return nodeValue;
  }
}
