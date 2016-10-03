/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.api;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.SortOrder;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Set;

@Path("mds/v1/metadata")
@Api(value = "MetadataV1",
        description = "Federated user metadata operations",
        produces = MediaType.APPLICATION_JSON,
        consumes = MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface MetadataV1 {
    @POST
    @Path("data")
    @ApiOperation(
            position = 1,
            value = "Returns the data metadata",
            notes = "Returns the data metadata")
    DataMetadataDto getDataMetadata(DataMetadataGetRequestDto metadataGetRequestDto);

    @GET
    @Path("definition/list")
    @ApiOperation(
            position = 2,
            value = "Returns the definition metadata",
            notes = "Returns the definition metadata")
    List<DefinitionMetadataDto> getDefinitionMetadataList(
            @ApiParam(value = "Sort the list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "has lifetime set", required = false)
            @DefaultValue("false") @QueryParam("lifetime")
            Boolean lifetime,
            @ApiParam(value = "Type of the metadata item. Values: database, table, partition", required = false)
            @QueryParam("type")
            String type,
            @ApiParam(value = "Text that matches the name of the metadata (accepts sql wildcards)", required = false)
            @QueryParam("name")
            String name,
            @ApiParam(value = "Set of data property names. Filters the returned list that only contains the given property names", required = false)
            @QueryParam("data-property")
            Set<String> dataProperties
            );

    @GET
    @Path("searchByOwners")
    @ApiOperation(
            position = 3,
            value = "Returns the qualified names owned by the given owners",
            notes = "Returns the qualified names owned by the given owners")
    List<QualifiedName> searchByOwners(
            @ApiParam(value = "Set of owners", required = true)
            @QueryParam("owner")
            Set<String> owners
    );

    @DELETE
    @Path("definition")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            position = 4,
            value = "Deletes the given definition metadata")
    void deleteDefinitionMetadata(
            @ApiParam(value = "Name of definition metadata to be deleted", required = true)
            @QueryParam("name")
            QualifiedName name,
            @ApiParam(value = "If true, deletes the metadata without checking if the database/table/partition exists", required = false)
            @DefaultValue("false") @QueryParam("force")
            Boolean force
    );
    @DELETE
    @Path("data/process")
    @Consumes(MediaType.APPLICATION_JSON)
    Response processDeletedDataMetadata();
}
