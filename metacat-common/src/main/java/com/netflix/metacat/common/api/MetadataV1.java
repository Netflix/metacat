/*
 *
 *  Copyright 2016 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.api;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.SortOrder;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

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

/**
 * API to manipulate user metadata.
 *
 * @author amajumdar
 */
@Path("mds/v1/metadata")
@Api(value = "MetadataV1",
    description = "Federated user metadata operations",
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface MetadataV1 {
    /**
     * Returns the data metadata.
     *
     * @param metadataGetRequestDto metadata request
     * @return data metadata
     */
    @POST
    @Path("data")
    @ApiOperation(
        position = 1,
        value = "Returns the data metadata",
        notes = "Returns the data metadata"
    )
    DataMetadataDto getDataMetadata(DataMetadataGetRequestDto metadataGetRequestDto);

    /**
     * Returns the list of definition metadata.
     *
     * @param sortBy         Sort the list by this value
     * @param sortOrder      Sorting order to use
     * @param offset         Offset of the list returned
     * @param limit          Size of the list
     * @param lifetime       has lifetime set
     * @param type           Type of the metadata item. Values: database, table, partition
     * @param name           Text that matches the name of the metadata (accepts sql wildcards)
     * @param dataProperties Set of data property names.
     *                       Filters the returned list that only contains the given property names
     * @return list of definition metadata
     */
    @GET
    @Path("definition/list")
    @ApiOperation(
        position = 2,
        value = "Returns the definition metadata",
        notes = "Returns the definition metadata"
    )
    List<DefinitionMetadataDto> getDefinitionMetadataList(
        @ApiParam(value = "Sort the list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "has lifetime set")
        @DefaultValue("false")
        @QueryParam("lifetime")
            Boolean lifetime,
        @ApiParam(value = "Type of the metadata item. Values: database, table, partition")
        @QueryParam("type")
            String type,
        @ApiParam(value = "Text that matches the name of the metadata (accepts sql wildcards)")
        @QueryParam("name")
            String name,
        @ApiParam(value = "Set of data property names. "
            + "Filters the returned list that only contains the given property names")
        @QueryParam("data-property")
            Set<String> dataProperties
    );

    /**
     * Returns the list of qualified names owned by the given owners.
     *
     * @param owners set of owners
     * @return the list of qualified names owned by the given owners
     */
    @GET
    @Path("searchByOwners")
    @ApiOperation(
        position = 3,
        value = "Returns the qualified names owned by the given owners",
        notes = "Returns the qualified names owned by the given owners"
    )
    List<QualifiedName> searchByOwners(
        @ApiParam(value = "Set of owners", required = true)
        @QueryParam("owner")
            Set<String> owners
    );

    /**
     * Delete the definition metadata for the given name.
     *
     * @param name  Name of definition metadata to be deleted
     * @param force If true, deletes the metadata without checking if the database/table/partition exists
     */
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
        @ApiParam(value = "If true, deletes the metadata without checking if the database/table/partition exists")
        @DefaultValue("false")
        @QueryParam("force")
            Boolean force
    );

    /**
     * Deletes the data metadata marked for deletion.
     *
     * @return response
     */
    @DELETE
    @Path("data/process")
    @Consumes(MediaType.APPLICATION_JSON)
    Response processDeletedDataMetadata();
}
