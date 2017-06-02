/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package com.netflix.metacat.client.api;

import com.netflix.metacat.common.dto.ResolveByUriRequestDto;
import com.netflix.metacat.common.dto.ResolveByUriResponseDto;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * ResolverV1.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Path("mds/v1/resolver")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface ResolverV1 {
    /**
     * resolveByUri.
     *
     * @param prefixSearch           use prefix search
     * @param resolveByUriRequestDto resolveByUriRequest
     * @return response from uri search
     */
    @POST
    ResolveByUriResponseDto resolveByUri(
        @DefaultValue("false")
        @QueryParam("prefixSearch")
            Boolean prefixSearch,
        ResolveByUriRequestDto resolveByUriRequestDto);

    /**
     * isUriUsedMoreThanOnce.
     *
     * @param prefixSearch           use prefix search
     * @param resolveByUriRequestDto resolveByUriRequest
     * @return response of check if a uri used more than once
     */
    @POST
    @Path("isUriUsedMoreThanOnce")
    Response isUriUsedMoreThanOnce(
        @DefaultValue("false")
        @QueryParam("prefixSearch")
            Boolean prefixSearch,
        ResolveByUriRequestDto resolveByUriRequestDto);
}
