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

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;

@Path("/druid/v1/namespaces")
public class NamespacesCacheResource
{
  private static final Logger log = new Logger(NamespacesCacheResource.class);
  private final NamespaceExtractionCacheManager namespaceExtractionCacheManager;

  @Inject
  public NamespacesCacheResource(final NamespaceExtractionCacheManager namespaceExtractionCacheManager){
    this.namespaceExtractionCacheManager = namespaceExtractionCacheManager;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getNamespaces(){
    try{
      return Response.ok().entity(namespaceExtractionCacheManager.getKnownNamespaceNames()).build();
    }catch (Exception ex){
      log.error("Can not get the list of known namespaces");
      return Response.serverError().entity(Strings.nullToEmpty(ex.getMessage())).build();
    }
  }

  @POST
  @Path("/add")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response namespacePost(final ExtractionNamespace namespace) {
    Collection<ExtractionNamespace> namespaces = namespaceExtractionCacheManager.getKnownNamespaces();
    namespaces.add(namespace);
    try {
      namespaceExtractionCacheManager.scheduleOrUpdate(namespaces);

      return Response.ok().entity(ImmutableMap.of("namespace", namespace.getNamespace())).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(ImmutableMap.of("error", e.getMessage()))
          .build();
    }
  }

  @POST
  @Path("/delete")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response namespaceDelete(final ExtractionNamespace namespace) {
    Collection<ExtractionNamespace> namespaces = namespaceExtractionCacheManager.getKnownNamespaces();
    namespaces.remove(namespace);
    try {
      namespaceExtractionCacheManager.scheduleOrUpdate(namespaces);

      return Response.ok().entity(ImmutableMap.of("namespace", namespace.getNamespace())).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(ImmutableMap.of("error", e.getMessage()))
          .build();
    }
  }
}
