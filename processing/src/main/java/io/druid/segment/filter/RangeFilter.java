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

package io.druid.segment.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.query.topn.AlphaNumericTopNMetricSpec;
import io.druid.query.topn.LexicographicTopNMetricSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;

import java.util.Comparator;

/**
 */
public class RangeFilter extends DimensionPredicateFilter
{
  private final String dimension;
  private final String min, max;
  private final boolean minIncluded, maxIncluded;
  private final boolean alphaNumeric;

  public RangeFilter(
      String dimension,
      String min,
      String max,
      Boolean minIncluded,
      Boolean maxIncluded,
      Boolean alphaNumeric
  )
  {
    super(dimension, new RangePredicate(min, max, minIncluded, maxIncluded, alphaNumeric));
    this.dimension = dimension;
    this.min = min;
    this.max = max;
    this.minIncluded = minIncluded;
    this.maxIncluded = maxIncluded;
    this.alphaNumeric = alphaNumeric;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    if (alphaNumeric) {
      return super.getBitmapIndex(selector);
    } else {
      Indexed<String> dimValues = selector.getDimensionValues(dimension);
      ImmutableBitmap retBitmap = selector.getBitmapFactory().makeEmptyImmutableBitmap();
      if (dimValues == null) {
        return retBitmap;
      }

      int minId = 0;
      int maxId = dimValues.size() - 1;

      if (min != null) {
        minId = dimValues.indexOf(min);
        minId = minId < 0 ? -minId - 1 : (minIncluded ? minId : minId + 1);
      }
      if (max != null) {
        maxId = dimValues.indexOf(max);
        maxId = maxId < 0 ? -maxId - 2 : (maxIncluded ? maxId : maxId - 1);
      }
      for (int id = minId; id <= maxId; id++) {
        String value = dimValues.get(id);
        ImmutableBitmap bitmap = selector.getBitmapIndex(dimension, value);
        retBitmap = retBitmap.union(bitmap);
      }
      return retBitmap;
    }
  }

  private static class RangePredicate implements Predicate<String>
  {
    private final String min, max;
    private final boolean minIncluded, maxIncluded;
    private final Comparator<String> comparator;

    public RangePredicate(String min, String max, Boolean minIncluded, Boolean maxIncluded, Boolean alphaNumeric)
    {
      this.min = min;
      this.max = max;
      this.minIncluded = minIncluded;
      this.maxIncluded = maxIncluded;
      comparator = alphaNumeric ? new LexicographicTopNMetricSpec(null).getComparator(null, null) :
          new AlphaNumericTopNMetricSpec(null).getComparator(null, null);
    }

    @Override
    public boolean apply(String input)
    {
      boolean checkMin = true;
      boolean checkMax = true;
      if (input == null) {
        return false;
      }
      if (min != null) {
        checkMin = minIncluded ? comparator.compare(min, input) <= 0 : comparator.compare(min, input) < 0;
      }
      if (max != null) {
        checkMax = maxIncluded ? comparator.compare(input, max) <= 0 : comparator.compare(input, max) < 0;
      }
      return checkMin && checkMax;
    }
  }
}
