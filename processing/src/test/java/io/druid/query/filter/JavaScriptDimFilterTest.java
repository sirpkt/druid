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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class JavaScriptDimFilterTest
{

  @Test
  public void testGetCacheKey1()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", null, "fn");
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", null, "mfn");
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKey2()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter(null, new String[]{"dim1", "dim2"}, "fn");
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter(null, new String[]{"dim1", "dim"}, "2fn");
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));
  }
}
