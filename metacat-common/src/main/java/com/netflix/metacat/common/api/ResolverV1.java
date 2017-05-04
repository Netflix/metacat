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


package com.netflix.metacat.common.api;

import com.netflix.metacat.common.dto.ResolveByUriRequestDto;
import com.netflix.metacat.common.dto.ResolveByUriResponseDto;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

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
@Api(value = "ResolverV1",
        description = "Metadata resolver operations",
        produces = MediaType.APPLICATION_JSON,
        consumes = MediaType.APPLICATION_JSON)
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
    @ApiOperation(
            position = 0,
            value = "Returns the list of qualified names of tables and partitions containing the given URI path",
            notes = "Returns the list of qualified names of tables and partitions containing the given URI path")
    ResolveByUriResponseDto resolveByUri(
            @ApiParam(value = "do prefix search for URI", required = false)
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
    @ApiOperation(
            position = 1,
            value = "Returns status 200 if the given URI is being referred more than once."
                    + " Returns status 404 if the given URI is not found or not being referred more than once.",
            notes = "Returns status 200 if the given URI is being referred more than once."
                    + " Returns status 404 if the given URI is not found or not being referred more than once.")
    Response isUriUsedMoreThanOnce(
            @ApiParam(value = "do prefix search for URI", required = false)
            @DefaultValue("false")
            @QueryParam("prefixSearch")
                    Boolean prefixSearch,
            ResolveByUriRequestDto resolveByUriRequestDto);
}
