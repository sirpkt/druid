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

package io.druid.segment;

import io.druid.data.input.impl.DimensionSchema;
import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import org.joda.time.Interval;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  Indexed<String> getDimensionNames();

  Indexed<DimensionSchema> getDimensions();

  Indexed<String> getMetricNames();

  Indexed<Comparable> getDimValueLookup(DimensionSchema dimension);

  Iterable<Rowboat> getRows();

  IndexedInts getBitmapIndex(DimensionSchema dimension, Comparable value);

  BitmapIndexSeeker getBitmapIndexSeeker(DimensionSchema dimension);

  String getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);
}
