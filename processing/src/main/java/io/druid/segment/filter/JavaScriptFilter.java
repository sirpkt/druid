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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ListIndexed;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import java.util.Arrays;
import java.util.Iterator;

public class JavaScriptFilter implements Filter
{
  private final JavaScriptPredicate predicate;
  private final String[] dimension;
  protected static final ListIndexed EMPTY_STR_DIM_VAL = new ListIndexed<>(Arrays.asList(new String[]{null}), String.class);

  public JavaScriptFilter(String[] dimension, final String script)
  {
    this.dimension = dimension;
    this.predicate = new JavaScriptPredicate(script);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    final Context cx = Context.enter();
    try {
      ImmutableBitmap bitmap;

      boolean allEmptyDimensions = true;
      Indexed<String> [] dimValuesList = new Indexed[dimension.length];
      Iterator<String> [] dimValuesIterator = new Iterator[dimension.length];
      String[] currentDim = new String[dimension.length];
      for(int idx = 0; idx < dimension.length; idx++) {
        dimValuesList[idx] = selector.getDimensionValues(dimension[idx]);
        if (dimValuesList[idx].size() > 0) {
          allEmptyDimensions = false;
        } else {
          dimValuesList[idx] = EMPTY_STR_DIM_VAL;
        }
        dimValuesIterator[idx] = dimValuesList[idx].iterator();
        if (idx != 0) {
          currentDim[idx] = dimValuesIterator[idx].next();
        }
      }

      bitmap = selector.getBitmapFactory().makeEmptyImmutableBitmap();
      if (!allEmptyDimensions) {
        int iteratingIndex = 0;
        while(true) {
          // advance iterator
          Iterator<String> iterator = dimValuesIterator[iteratingIndex];
          if (iterator.hasNext()) {
            currentDim[iteratingIndex] = iterator.next();
            if (iteratingIndex > 0) {
              // reset inner loop iterators
              for (int idx = 0; idx < iteratingIndex; idx++) {
                dimValuesIterator[idx] = dimValuesList[idx].iterator();
                currentDim[idx] = dimValuesIterator[idx].next();
              }
              // move to the most inner loop
              iteratingIndex = 0;
            }
            if (predicate.applyInContext(cx, currentDim))
            {
              // update bitmap
              ImmutableBitmap overlap = null;
              for (int idx = 0; idx < dimension.length; idx++) {
                ImmutableBitmap dimBitMap = selector.getBitmapIndex(dimension[idx], currentDim[idx]);
                overlap = (overlap == null) ? dimBitMap : overlap.intersection(dimBitMap);
              }
              bitmap = bitmap.union(overlap);
            }

          } else {
            iteratingIndex++;
            if (iteratingIndex == dimension.length) break;
          }
        }
      }
      return bitmap;
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return factory.makeValueMatcher(dimension, predicate);
  }

  static class JavaScriptPredicate implements Predicate<String[]>
  {
    final ScriptableObject scope;
    final Function fnApply;
    final String script;

    public JavaScriptPredicate(final String script)
    {
      Preconditions.checkNotNull(script, "script must not be null");
      this.script = script;

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();

        fnApply = cx.compileFunction(scope, script, "script", 1, null);
      }
      finally {
        Context.exit();
      }
    }

    @Override
    public boolean apply(final String[] input)
    {
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return applyInContext(cx, input);
      }
      finally {
        Context.exit();
      }

    }

    public boolean applyInContext(Context cx, String[] input)
    {
      return Context.toBoolean(fnApply.call(cx, scope, scope, input));
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

      JavaScriptPredicate that = (JavaScriptPredicate) o;

      if (!script.equals(that.script)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return script.hashCode();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

}
