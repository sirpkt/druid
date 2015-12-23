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
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.dimension.DimensionSchema;
import io.druid.segment.dimension.DimensionType;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ExtractionFilter implements Filter
{
  private final String dimension;
  private final DimensionType dimType;
  private final Comparable value;
  private final ExtractionFn fn;

  public ExtractionFilter(String dimension, Comparable value, ExtractionFn fn)
  {
    this.dimension = dimension;
    this.dimType = DimensionSchema.fromString(dimension).getType();
    this.value = value == null ? dimType.getNullReplacement() : value;
    this.fn = fn;
  }

  private List<Filter> makeFilters(BitmapIndexSelector selector)
  {
    Indexed<Comparable> allDimVals = selector.getDimensionValues(dimension);
    final List<Filter> filters = Lists.newArrayList();
    if (allDimVals == null) {
      allDimVals = new Indexed<Comparable>()
      {
        @Override
        public Iterator<Comparable> iterator()
        {
          return null;
        }

        @Override
        public Class<? extends Comparable> getClazz()
        {
          return null;
        }

        @Override
        public int size() { return 1; }

        @Override
        public Comparable get(int index) { return null;}

        @Override
        public int indexOf(Comparable value)
        {
          return 0;
        }
      };
    }

    for (int i = 0; i < allDimVals.size(); i++) {
      Comparable dimVal = allDimVals.get(i);
      if (value.equals(dimType.getNullReplaced(fn.apply(dimVal)))) {
        filters.add(new SelectorFilter(dimension, dimVal));
      }
    }

    return filters;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    final List<Filter> filters = makeFilters(selector);
    if (filters.isEmpty()) {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }
    return new OrFilter(makeFilters(selector)).getBitmapIndex(selector);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        dimension, new Predicate<Comparable>()
        {
          @Override
          public boolean apply(Comparable input)
          {
            // Assuming that a null/absent/empty dimension are equivalent from the druid perspective
            return value.equals(dimType.getNullReplaced(fn.apply(dimType.getNullRestored(input))));
          }
        }
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    final DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimension, null);
    if (dimensionSelector == null) {
      return new BooleanValueMatcher(value.equals(dimType.getNullReplaced(fn.apply(null))));
    } else {
      final BitSet bitSetOfIds = new BitSet(dimensionSelector.getValueCardinality());
      for (int i = 0; i < dimensionSelector.getValueCardinality(); i++) {
        if (value.equals(dimType.getNullReplaced(fn.apply(dimensionSelector.lookupName(i))))) {
          bitSetOfIds.set(i);
        }
      }
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = dimensionSelector.getRow();
          final int size = row.size();
          for (int i = 0; i < size; ++i) {
            if (bitSetOfIds.get(row.get(i))) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }

}
