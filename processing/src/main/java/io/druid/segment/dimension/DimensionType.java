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

package io.druid.segment.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.segment.column.ValueType;

public enum DimensionType
{
  STRING("STRING", "java.lang.String", ValueType.STRING, ""),
  FLOAT("FLOAT", "java.lang.Float", ValueType.FLOAT, Float.NaN),
  LONG("LONG", "java.lang.Long", ValueType.LONG, null);

  private final String name;
  private final Class clazz;
  private final ValueType type;
  private final Comparable nullReplacement;

  private DimensionType(String name, String clazz, ValueType type, Comparable nullReplacement) {
    this.name = name;
    Class loadedClass = null;
    try {
      loadedClass = Class.forName(clazz);
    } catch (ClassNotFoundException e) {
      // However, should not reach here
      e.printStackTrace();
    }
    this.nullReplacement = nullReplacement;
    this.clazz = loadedClass;
    this.type = type;
  }

  @JsonValue
  @Override
  public String toString()
  {
    return name.toLowerCase();
  }

  @JsonCreator
  public static DimensionType fromString(String name)
  {
    return valueOf(name.toUpperCase());
  }

  public Class<? extends Comparable> getClazz()
  {
    return clazz;
  }

  public Comparable getNullReplacement()
  {
    return nullReplacement;
  }

  public Comparable getNullReplaced(Comparable value)
  {
    return value == null ? nullReplacement : value;
  }

  public Comparable getNullRestored(Comparable value)
  {
    return value == nullReplacement ? null : value;
  }

  public Comparable fromStringValue(String str) {
    if (str == null || "".equals(str)) {
      return nullReplacement;
    }

    switch (name) {
      case "FLOAT":
        return Float.parseFloat(str);
      case "LONG":
        return Long.parseLong(str);
    }

    return str;
  }

  public ValueType getType()
  {
    return type;
  }

  public static boolean isValid(String name)
  {
    try {
      DimensionType type = fromString(name);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }
}
