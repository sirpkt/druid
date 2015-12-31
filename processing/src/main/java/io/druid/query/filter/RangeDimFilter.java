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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import com.sun.istack.internal.Nullable;

import java.nio.ByteBuffer;

/**
 */
public class RangeDimFilter implements DimFilter
{
    private final String dimension;
    private final String min, max;
    private final boolean minIncluded, maxIncluded;
    private final boolean alphaNumeric;

    @JsonCreator
    public RangeDimFilter(
            @JsonProperty("dimension") String dimension,
            @Nullable @JsonProperty("min") String min,
            @Nullable @JsonProperty("max") String max,
            @Nullable @JsonProperty("minIncluded") Boolean minIncluded,
            @Nullable @JsonProperty("maxIncluded") Boolean maxIncluded,
            @Nullable @JsonProperty("alphaNumeric") Boolean alphaNumeric
    )
    {
        Preconditions.checkArgument(dimension != null, "dimension must not be null");
        Preconditions.checkArgument(min != null || max != null, "one of min, max must not be null");

        this.dimension = dimension;
        this.min = min;
        this.max = max;
        this.minIncluded = (minIncluded == null) ? false : minIncluded;
        this.maxIncluded = (maxIncluded == null) ? false : maxIncluded;
        this.alphaNumeric = (alphaNumeric == null) ? false : alphaNumeric;
    }

    @Override
    public byte[] getCacheKey()
    {
        byte[] dimensionBytes = StringUtils.toUtf8(dimension);
        byte[] minBytes = (min == null) ? new byte[]{} : StringUtils.toUtf8(min);
        byte[] maxBytes = (max == null) ? new byte[]{} : StringUtils.toUtf8(max);
        byte minIncludedByte = minIncluded ? (byte)1 : 0x0;
        byte maxIncludedByte = maxIncluded ? (byte)1 : 0x0;
        byte alphaNumericByte = alphaNumeric ? (byte)1 : 0x0;

        return ByteBuffer.allocate(7 + dimensionBytes.length + minBytes.length + maxBytes.length)
                .put(DimFilterCacheHelper.RANGE_CACHE_ID)
                .put(minIncludedByte)
                .put(maxIncludedByte)
                .put(alphaNumericByte)
                .put(DimFilterCacheHelper.STRING_SEPARATOR)
                .put(dimensionBytes)
                .put(DimFilterCacheHelper.STRING_SEPARATOR)
                .put(minBytes)
                .put(DimFilterCacheHelper.STRING_SEPARATOR)
                .put(maxBytes)
                .array();
    }

    @JsonProperty
    public String getDimension()
    {
        return dimension;
    }

    @JsonProperty
    public String getMin()
    {
        return min;
    }

    @JsonProperty
    public String getMax()
    {
        return max;
    }

    @JsonProperty
    public boolean isMinIncluded()
    {
        return minIncluded;
    }

    @JsonProperty
    public boolean isMaxIncluded()
    {
        return maxIncluded;
    }

    @JsonProperty
    public boolean isAlphaNumeric()
    {
        return alphaNumeric;
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

        RangeDimFilter that = (RangeDimFilter) o;

        if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
            return false;
        }
        if (min != null ? !min.equals(that.min) : that.min != null) {
            return false;
        }
        if (max != null ? !max.equals(that.max) : that.max != null) {
            return false;
        }
        if (minIncluded != that.minIncluded) {
            return false;
        }
        if (maxIncluded != that.maxIncluded) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = dimension != null ? dimension.hashCode() : 0;
        result = 31 * result + (min != null ? min.hashCode() : 0);
        result = 31 * result + (max != null ? max.hashCode() : 0);
        result = 31 * result + (minIncluded ? 1 : 0);
        result = 31 * result + (maxIncluded ? 1 : 0);
        result = 31 * result + (alphaNumeric ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        if (min == null) {
            return String.format("%s %s %s", dimension, maxIncluded?"<=":"<",max);
        }
        if (max == null) {
            return String.format("%s %s %s", min, minIncluded?"<=":"<", dimension);
        }
        return String.format("%s %s %s %s %s", min, minIncluded?"<=":"<", dimension, maxIncluded?"<=":"<",max);
    }
}
