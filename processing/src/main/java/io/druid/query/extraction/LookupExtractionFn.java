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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.metamx.common.StringUtils;
import io.druid.segment.dimension.DimensionType;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class LookupExtractionFn extends FunctionalExtraction
{
  private static final byte CACHE_TYPE_ID = 0x7;

  private final LookupExtractor lookup;

  @JsonCreator
  public LookupExtractionFn(
      @JsonProperty("lookup")
      final LookupExtractor lookup,
      @JsonProperty("retainMissingValue")
      final boolean retainMissingValue,
      @Nullable
      @JsonProperty("replaceMissingValueWith")
      final Comparable replaceMissingValueWith,
      @JsonProperty("injective")
      final boolean injective,
      @JsonProperty("dimType")
      final String dimType
  )
  {
    super(
        new Function<Comparable, Comparable>()
        {
          @Nullable
          @Override
          public Comparable apply(Comparable input)
          {
            return lookup.apply(DimensionType.fromString(dimType).getNullReplaced(input));
          }
        },
        retainMissingValue,
        replaceMissingValueWith,
        injective,
        dimType
    );
    this.lookup = lookup;
  }


  @JsonProperty
  public LookupExtractor getLookup()
  {
    return lookup;
  }

  @Override
  @JsonProperty
  public boolean isRetainMissingValue() {return super.isRetainMissingValue();}

  @Override
  @JsonProperty
  public Comparable getReplaceMissingValueWith() {return super.getReplaceMissingValueWith();}

  @Override
  @JsonProperty
  public boolean isInjective()
  {
    return super.isInjective();
  }

  @Override
  @JsonProperty
  public String getDimType()
  {
    return super.getDimType();
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(CACHE_TYPE_ID);
      outputStream.write(lookup.getCacheKey());
      if (getReplaceMissingValueWith() != null) {
        outputStream.write(StringUtils.toUtf8(String.valueOf(getReplaceMissingValueWith())));
      }
      outputStream.write(isInjective() ? 1 : 0);
      outputStream.write(isRetainMissingValue() ? 1 : 0);
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

    LookupExtractionFn that = (LookupExtractionFn) o;

    return lookup.equals(that.lookup);

  }

  @Override
  public int hashCode()
  {
    return lookup.hashCode();
  }
}
