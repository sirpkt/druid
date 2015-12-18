/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class DimensionSchema implements Comparable<DimensionSchema>
{
  private final String name;
  private final DimensionType type;

  @JsonCreator
  public DimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type
  )
  {
    this.name = name;
    this.type = DimensionType.isValid(type) ? DimensionType.fromString(type) : null;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public DimensionType getType()
  {
    return type;
  }

  @Override
  public String toString() {
    return name + ":" + type;
  }

  public static DimensionSchema fromString(String schemaString)
  {
    String[] dimSchema = schemaString.split(":");
    Preconditions.checkArgument(dimSchema.length == 2 || dimSchema.length == 1,
            "%s is not valid schema string", schemaString);

    return dimSchema.length == 2 ? new DimensionSchema(dimSchema[0], dimSchema[1]) :
            new DimensionSchema(schemaString, "String");
  }

  public static List<DimensionSchema> fromStringList(List<String> schemaStringList)
  {
    return Lists.transform(
        schemaStringList,
        new Function<String, DimensionSchema>() {
          @Nullable
          @Override
          public DimensionSchema apply(@Nullable String s) {
            return fromString(s);
          }
        }
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionSchema that = (DimensionSchema) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!type.equals(that.type)) {
      return false;
    }

    return true;
  }

  @Override
  public int compareTo(DimensionSchema dimensionSchema)
  {
    return this.toString().compareTo(dimensionSchema.toString());
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }
}
