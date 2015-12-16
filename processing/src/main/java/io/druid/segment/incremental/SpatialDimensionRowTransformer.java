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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.SpatialDimensionSchema;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * We throw away all invalid spatial dimensions
 */
public class SpatialDimensionRowTransformer implements Function<InputRow, InputRow>
{
  private static final Joiner JOINER = Joiner.on(",");
  private static final Splitter SPLITTER = Splitter.on(",");

  private final Map<String, SpatialDimensionSchema> spatialDimensionMap;
  private final Set<String> spatialPartialDimNames;

  public SpatialDimensionRowTransformer(List<SpatialDimensionSchema> spatialDimensions)
  {
    this.spatialDimensionMap = Maps.newHashMap();
    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      if (this.spatialDimensionMap.put(spatialDimension.getDimName(), spatialDimension) != null) {
        throw new ISE("Duplicate spatial dimension names found! Check your schema yo!");
      }
    }
    this.spatialPartialDimNames = Sets.newHashSet(
        Iterables.concat(
            Lists.transform(
                spatialDimensions,
                new Function<SpatialDimensionSchema, List<String>>()
                {
                  @Override
                  public List<String> apply(SpatialDimensionSchema input)
                  {
                    return input.getDims();
                  }
                }
            )
        )
    );
  }

  @Override
  public InputRow apply(final InputRow row)
  {
    final Map<DimensionSchema, List<Comparable>> spatialLookup = Maps.newHashMap();

    // remove all spatial dimensions
    final List<DimensionSchema> finalDims = Lists.newArrayList(
        Iterables.filter(
            row.getDimensions(),
            new Predicate<DimensionSchema>()
            {
              @Override
              public boolean apply(DimensionSchema input)
              {
                return !spatialDimensionMap.containsKey(input.getName()) && !spatialPartialDimNames.contains(input.getName());
              }
            }
        )
    );

    InputRow retVal = new InputRow()
    {
      @Override
      public List<DimensionSchema> getDimensions()
      {
        return finalDims;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public DateTime getTimestamp()
      {
        return row.getTimestamp();
      }

      @Override
      public List<Comparable> getDimension(DimensionSchema dimension)
      {
        List<Comparable> retVal = spatialLookup.get(dimension);
        return (retVal == null) ? row.getDimension(dimension) : retVal;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return row.getRaw(dimension);
      }

      @Override
      public long getLongMetric(String metric)
      {
        try {
          return row.getLongMetric(metric);
        }
        catch (ParseException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public float getFloatMetric(String metric)
      {
        try {
          return row.getFloatMetric(metric);
        }
        catch (ParseException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString()
      {
        return row.toString();
      }

      @Override
      public int compareTo(Row o)
      {
        return getTimestamp().compareTo(o.getTimestamp());
      }
    };

    for (Map.Entry<String, SpatialDimensionSchema> entry : spatialDimensionMap.entrySet()) {
      final String spatialDimName = entry.getKey();
      final SpatialDimensionSchema spatialDim = entry.getValue();
      // TODO: fix the type of spatial dimension
      final DimensionSchema spatialDimSchema = new DimensionSchema(spatialDimName, "String");

      List<Comparable> dimVals = row.getDimension(spatialDimSchema);
      if (dimVals != null && !dimVals.isEmpty()) {
        if (dimVals.size() != 1) {
          throw new ISE("Spatial dimension value must be in an array!");
        }
        if (isJoinedSpatialDimValValid((String)dimVals.get(0))) {
          spatialLookup.put(spatialDimSchema, dimVals);
          finalDims.add(spatialDimSchema);
        }
      } else {
        List<Comparable> spatialDimVals = Lists.newArrayList();
        for (String dim : spatialDim.getDims()) {
          // TODO: fix the type of spatial dimension
          final DimensionSchema dimSchema = new DimensionSchema(dim, "String");
          List<Comparable> partialDimVals = row.getDimension(dimSchema);
          if (isSpatialDimValsValid(partialDimVals)) {
            spatialDimVals.addAll(partialDimVals);
          }
        }

        if (spatialDimVals.size() == spatialDim.getDims().size()) {
          spatialLookup.put(spatialDimSchema, Arrays.asList((Comparable)JOINER.join(spatialDimVals)));
          finalDims.add(spatialDimSchema);
        }
      }
    }

    return retVal;
  }

  private boolean isSpatialDimValsValid(List<Comparable> dimVals)
  {
    if (dimVals == null || dimVals.isEmpty()) {
      return false;
    }
    for (Comparable dimVal : dimVals) {
      if (tryParseFloat((String)dimVal) == null) {
        return false;
      }
    }
    return true;
  }

  private boolean isJoinedSpatialDimValValid(String dimVal)
  {
    if (dimVal == null || dimVal.isEmpty()) {
      return false;
    }
    Iterable<String> dimVals = SPLITTER.split(dimVal);
    for (String val : dimVals) {
      if (tryParseFloat(val) == null) {
        return false;
      }
    }
    return true;
  }

  private static Float tryParseFloat(String val) {
    try {
      return Float.parseFloat(val);
    } catch (NullPointerException | NumberFormatException e) {
      return null;
    }
  }
}
