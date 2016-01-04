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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class JavaScriptDimFilterSerDesrTest
{
  private static ObjectMapper mapper;

  private final String actualJSFilter1 = "{\"type\":\"javascript\",\"dimension\":\"dimTest\",\"dimensions\":null,\"function\":\"function(d1) { return true }\"}";
  private final String actualJSFilter2 = "{\"type\":\"javascript\",\"dimension\":null,\"dimensions\":[\"dimTest1\",\"dimTest2\"],\"function\":\"function(d1, d2) { return d1 === d2 }\"}";
  private final String actualJSFilterSkipNull1 = "{\"type\":\"javascript\",\"dimension\":\"dimTest\",\"function\":\"function(d1) { return true }\"}";
  private final String actualJSFilterSkipNull2 = "{\"type\":\"javascript\",\"dimensions\":[\"dimTest1\",\"dimTest2\"],\"function\":\"function(d1, d2) { return d1 === d2 }\"}";

  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization1() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(actualJSFilter1);
    final JavaScriptDimFilter expectedJSDimFilter =
        new JavaScriptDimFilter("dimTest", null, "function(d1) { return true }");
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testDeserialization2() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(actualJSFilter2);
    final JavaScriptDimFilter expectedJSDimFilter =
        new JavaScriptDimFilter(null, new String[]{"dimTest1", "dimTest2"}, "function(d1, d2) { return d1 === d2 }");
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testDeserialization3() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(actualJSFilterSkipNull1);
    final JavaScriptDimFilter expectedJSDimFilter =
        new JavaScriptDimFilter("dimTest", null, "function(d1) { return true }");
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testDeserialization4() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(actualJSFilterSkipNull2);
    final JavaScriptDimFilter expectedJSDimFilter =
        new JavaScriptDimFilter(null, new String[]{"dimTest1", "dimTest2"}, "function(d1, d2) { return d1 === d2 }");
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testSerialization1() throws IOException
  {
    final JavaScriptDimFilter JSDimFilter = new JavaScriptDimFilter("dimTest", null, "function(d1) { return true }");
    final String expectedJSDimFilter = mapper.writeValueAsString(JSDimFilter);
    Assert.assertEquals(expectedJSDimFilter, actualJSFilter1);
  }

  @Test
  public void testSerialization2() throws IOException
  {
    final JavaScriptDimFilter JSDimFilter =
        new JavaScriptDimFilter(null, new String[]{"dimTest1", "dimTest2"}, "function(d1, d2) { return d1 === d2 }");
    final String expectedJSDimFilter = mapper.writeValueAsString(JSDimFilter);
    Assert.assertEquals(expectedJSDimFilter, actualJSFilter2);
  }
}
