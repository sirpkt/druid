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

import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.dimension.DimensionSchema;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts = new ConcurrentSkipListMap<>();
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;

  private String outOfRowsReason = null;

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);
    this.maxRowCount = maxRowCount;
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, maxRowCount);
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension)
  {
    return new OnHeapDimDim(DimensionSchema.fromString(dimension));
  }

  @Override
  protected Aggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    return new Aggregator[metrics.length];
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    final Integer priorIndex = facts.get(key);

    Aggregator[] aggs;

    if (null != priorIndex) {
      aggs = concurrentGet(priorIndex);
    } else {
      aggs = new Aggregator[metrics.length];

      for (int i = 0; i < metrics.length; i++) {
        final AggregatorFactory agg = metrics[i];
        aggs[i] = agg.factorize(
            makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics)
        );
      }
      final Integer rowIndex = indexIncrement.getAndIncrement();

      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Integer prev = facts.putIfAbsent(key, rowIndex);
      if (null == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = concurrentGet(prev);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    rowContainer.set(row);

    for (Aggregator agg : aggs) {
      synchronized (agg) {
        agg.aggregate();
      }
    }

    rowContainer.set(null);


    return numEntries.get();
  }

  protected Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  protected void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = String.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].get();
  }

  private static class OnHeapDimDim<T extends Comparable> implements DimDim<T>
  {
    private final Map<T, Integer> falseIds;
    private final Map<Integer, T> falseIdsReverse;
    private volatile T[] sortedVals = null;
    final ConcurrentMap<T, T> poorMansInterning = Maps.newConcurrentMap();
    private final Class clazz;

    public OnHeapDimDim(DimensionSchema dimensionSchema)
    {
      BiMap<T, Integer> biMap = Maps.synchronizedBiMap(HashBiMap.<T, Integer>create());
      falseIds = biMap;
      falseIdsReverse = biMap.inverse();
      this.clazz = dimensionSchema.getType().getClazz();
    }

    /**
     * Returns the interned String value to allow fast comparisons using `==` instead of `.equals()`
     *
     * @see io.druid.segment.incremental.IncrementalIndexStorageAdapter.EntryHolderValueMatcherFactory#makeValueMatcher(String, String)
     */
    public T get(T str)
    {
      T prev = poorMansInterning.putIfAbsent(str, str);
      return prev != null ? prev : str;
    }

    public int getId(T value)
    {
      final Integer id = falseIds.get(value);
      return id == null ? -1 : id;
    }

    public T getValue(int id)
    {
      return falseIdsReverse.get(id);
    }

    public boolean contains(T value)
    {
      return falseIds.containsKey(value);
    }

    public int size()
    {
      return falseIds.size();
    }

    public synchronized int add(T value)
    {
      int id = falseIds.size();
      falseIds.put(value, id);
      return id;
    }

    public int getSortedId(T value)
    {
      assertSorted();
      return Arrays.binarySearch(sortedVals, value);
    }

    public T getSortedValue(int index)
    {
      assertSorted();
      return sortedVals[index];
    }

    public void sort()
    {
      if (sortedVals == null) {
        sortedVals = (T[])Array.newInstance(this.clazz, falseIds.size());

        int index = 0;
        for (T value : falseIds.keySet()) {
          sortedVals[index++] = value;
        }
        Arrays.sort(sortedVals);
      }
    }

    private void assertSorted()
    {
      if (sortedVals == null) {
        throw new ISE("Call sort() before calling the getSorted* methods.");
      }
    }

    public boolean compareCannonicalValues(T s1, T s2)
    {
      return s1.equals(s2);
    }
  }
}
