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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.RangeDimFilter;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class RangeDimFilterTest
{
  private static final Map<String, String[]> DIM_VALS = ImmutableMap.<String, String[]>of(
      "foo", new String[]{"foo1", "foo2", "foo3"},
      "bar", new String[]{"1", "3", "7"}
  );

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory()},
        new Object[]{new RoaringBitmapFactory()}
    );
  }

  public RangeDimFilterTest(BitmapFactory bitmapFactory)
  {
    final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmap.add(1);
    this.foo1BitMap = bitmapFactory.makeImmutableBitmap(mutableBitmap);
    final MutableBitmap mutableBitmap2 = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmap2.add(2);
    this.foo2BitMap = bitmapFactory.makeImmutableBitmap(mutableBitmap2);
    final MutableBitmap mutableBitmap3 = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmap3.add(3);
    this.foo3BitMap = bitmapFactory.makeImmutableBitmap(mutableBitmap3);
    this.factory = bitmapFactory;
  }

  private final BitmapFactory factory;
  private final ImmutableBitmap foo1BitMap;
  private final ImmutableBitmap foo2BitMap;
  private final ImmutableBitmap foo3BitMap;

  private final BitmapIndexSelector BITMAP_INDEX_SELECTOR = new BitmapIndexSelector()
  {
    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      final String[] vals = DIM_VALS.get(dimension);
      return vals == null ? null : new ArrayIndexed<String>(vals, String.class);
    }

    @Override
    public int getNumRows()
    {
      return 3;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return factory;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, String value)
    {
      ImmutableBitmap bitmap = null;
      switch(value) {
        case "foo1": bitmap = foo1BitMap;
          break;
        case "foo2": bitmap = foo2BitMap;
          break;
        case "foo3": bitmap = foo3BitMap;
          break;
      }
      return bitmap;
    }

    @Override
    public ImmutableRTree getSpatialIndex(String dimension)
    {
      return null;
    }
  };

  @Test
  public void testEmpty()
  {
    RangeFilter rangeFilter = new RangeFilter(
        "foo", "foo4", "foo5", false, false, false
    );
    ImmutableBitmap immutableBitmap = rangeFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNull()
  {
    RangeFilter rangeFilter = new RangeFilter(
        "fooo", "foo1", "foo2", false, false, false
    );
    ImmutableBitmap immutableBitmap = rangeFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNormal1()
  {
    RangeFilter rangeFilter = new RangeFilter(
        "foo", "foo1", "foo3", false, false, false
    );
    ImmutableBitmap immutableBitmap = rangeFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(1, immutableBitmap.size());
  }

  @Test
  public void testNormal2()
  {
    RangeFilter rangeFilter = new RangeFilter(
        "foo", "foo1", "foo2", true, true, false
    );
    ImmutableBitmap immutableBitmap = rangeFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(2, immutableBitmap.size());
  }

  @Test
  public void testOr()
  {
    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new RangeDimFilter(
                    "foo", "foo1", "foo1", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        2,
        Filters.convertDimensionFilters(
            DimFilters.or(
                new RangeDimFilter(
                    "foo", "foo1", "foo1", true, true, false
                ),
                new RangeDimFilter(
                    "foo", "foo1", "foo2", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testAnd()
  {
    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new RangeDimFilter(
                    "foo", "foo1", "foo1", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        1,
        Filters.convertDimensionFilters(
            DimFilters.and(
                new RangeDimFilter(
                    "foo", "foo1", "foo1", true, true, false
                ),
                new RangeDimFilter(
                    "foo", "foo1", "foo2", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testNot()
  {

    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new RangeDimFilter(
                    "foo", "foo1", "foo1", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        2,
        Filters.convertDimensionFilters(
            DimFilters.not(
                new RangeDimFilter(
                    "foo", "foo2", "foo2", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        3,
        Filters.convertDimensionFilters(
            DimFilters.not(
                new RangeDimFilter(
                    "foo", "foo4", "foo5", true, true, false
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }
}
