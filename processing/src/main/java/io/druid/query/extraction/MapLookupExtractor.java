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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.StringUtils;
import io.druid.segment.dimension.DimensionType;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonTypeName("map")
public class MapLookupExtractor extends LookupExtractor
{
  private final Map<Comparable, Comparable> map;
  private final DimensionType dimType;

  @JsonCreator
  public MapLookupExtractor(
      @JsonProperty("map") Map<Comparable, Comparable> map,
      @JsonProperty("dimType") String dimType
  )
  {
    this.map = Preconditions.checkNotNull(map, "map");
    this.dimType = DimensionType.fromString(dimType);
  }

  @JsonProperty
  public Map<Comparable, Comparable> getMap()
  {
    return ImmutableMap.copyOf(map);
  }

  @JsonProperty
  public String getDimType() {
    return dimType.toString();
  }

  @Nullable
  @Override
  public Comparable apply(@NotNull Comparable val)
  {
    return map.get(val);
  }

  @Override
  public List<Comparable> unapply(final Comparable value)
  {
    return Lists.newArrayList(Maps.filterKeys(map, new Predicate<Comparable>()
    {
      @Override public boolean apply(@Nullable Comparable key)
      {
        return map.get(key).equals(dimType.getNullReplaced(value));
      }
    }).keySet());

  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      for (Map.Entry<Comparable, Comparable> entry : map.entrySet()) {
        final String key = String.valueOf(entry.getKey());
        final String val = String.valueOf(entry.getValue());
        if (!Strings.isNullOrEmpty(key)) {
          outputStream.write(StringUtils.toUtf8(key));
        }
        outputStream.write((byte)0xFF);
        if (!Strings.isNullOrEmpty(val)) {
          outputStream.write(StringUtils.toUtf8(val));
        }
        outputStream.write((byte)0xFF);
      }
      outputStream.write(StringUtils.toUtf8(getDimType()));
      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw Throwables.propagate(ex);
    }
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

    MapLookupExtractor that = (MapLookupExtractor) o;

    return map.equals(that.map);
  }

  @Override
  public int hashCode()
  {
    return map.hashCode();
  }
}
