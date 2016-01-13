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

package io.druid.server.namespace.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.*;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.guice.GuiceAnnotationIntrospector;
import io.druid.guice.GuiceInjectableValues;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.namespace.JDBCExtractionNamespaceFunctionFactory;
import io.druid.server.namespace.URIExtractionNamespaceFunctionFactory;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacesCacheResourceTest
{
  public static ObjectMapper registerTypes(
      final ObjectMapper mapper
  )
  {
    mapper.setInjectableValues(
        new GuiceInjectableValues(
            Guice.createInjector(
                ImmutableList.of(
                    new Module()
                    {
                      @Override
                      public void configure(Binder binder)
                      {
                        binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(mapper);
                        binder.bind(ObjectMapper.class).toInstance(mapper);
                      }
                    }
                )
            )
        )
    );

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    return mapper;
  }

  private static final ObjectMapper mapper = NamespacesCacheResourceTest.registerTypes(new DefaultObjectMapper());
  private static NamespaceExtractionCacheManager cacheManager;
  private static Lifecycle lifecycle;
  private static ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();
  private static NamespacesCacheResource cacheResource;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpStatic() throws Exception
  {
    final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> factoryMap =
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
            URIExtractionNamespace.class,
            new URIExtractionNamespaceFunctionFactory(
                ImmutableMap.<String, SearchableVersionedDataFinder>of(
                    "file",
                    new LocalFileTimestampVersionFinder()
                )
            ),
            JDBCExtractionNamespace.class, new JDBCExtractionNamespaceFunctionFactory()
        );
    lifecycle = new Lifecycle();
    cacheManager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle,
        new ConcurrentHashMap<String, Function<String, String>>(),
        new ConcurrentHashMap<String, Function<String, List<String>>>(),
        new NoopServiceEmitter(), factoryMap
    );
    cacheResource = new NamespacesCacheResource(cacheManager);
    fnCache.clear();
  }

  @AfterClass
  public static void tearDownStatic() throws Exception
  {
    lifecycle.stop();
  }

  @Test(timeout = 1_000)
  public void testAddNamespace() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final String URInamespace = "{ \n" +
                                "  \"type\":\"uri\",\n" +
                                "  \"namespace\":\"ns\",\n" +
                                "  \"uri\": \"" + tmpFile.toURI() + "\",\n" +
                                "  \"namespaceParseSpec\":\n" +
                                "    {\n" +
                                "      \"format\":\"simpleJson\"\n" +
                                "    }\n" +
                                "}";
    final ExtractionNamespace namespace = mapper.reader(ExtractionNamespace.class).readValue(URInamespace);

    Response response = cacheResource.namespacePost(namespace);
    Assert.assertEquals(200, response.getStatus());

    while(ImmutableList.of("ns").equals(cacheResource.getNamespaces().getEntity())) {
      Thread.sleep(1);
    }
  }

  @Test(timeout = 1_000)
  public void testDeleteNamespace() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final String URInamespace1 = "{ \n" +
        "  \"type\":\"uri\",\n" +
        "  \"namespace\":\"ns1\",\n" +
        "  \"uri\": \"" + tmpFile.toURI() + "\",\n" +
        "  \"namespaceParseSpec\":\n" +
        "    {\n" +
        "      \"format\":\"simpleJson\"\n" +
        "    }\n" +
        "}";
    final ExtractionNamespace namespace1 = mapper.reader(ExtractionNamespace.class).readValue(URInamespace1);
    final String URInamespace2 = "{ \n" +
        "  \"type\":\"uri\",\n" +
        "  \"namespace\":\"ns2\",\n" +
        "  \"uri\": \"" + tmpFile.toURI() + "\",\n" +
        "  \"namespaceParseSpec\":\n" +
        "    {\n" +
        "      \"format\":\"simpleJson\"\n" +
        "    }\n" +
        "}";
    final ExtractionNamespace namespace2 = mapper.reader(ExtractionNamespace.class).readValue(URInamespace2);

    Response response = cacheResource.namespacePost(namespace1);
    Assert.assertEquals(200, response.getStatus());

    response = cacheResource.namespacePost(namespace2);
    Assert.assertEquals(200, response.getStatus());

    while(ImmutableList.of("ns1", "ns2").equals(cacheResource.getNamespaces().getEntity())) {
      Thread.sleep(1);
    }

    response = cacheResource.namespaceDelete(namespace1);
    Assert.assertEquals(200, response.getStatus());

    while(ImmutableList.of("ns2").equals(cacheResource.getNamespaces().getEntity())) {
      Thread.sleep(1);
    }
  }
}
