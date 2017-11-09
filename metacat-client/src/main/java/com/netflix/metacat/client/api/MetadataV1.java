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
package com.netflix.metacat.client.api;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.SortOrder;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Set;

/**
 * API to manipulate user metadata.
 *
 * @author amajumdar
 */
@Path("mds/v1/metadata")
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
    List<DefinitionMetadataDto> getDefinitionMetadataList(
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        @DefaultValue("false")
        @QueryParam("lifetime")
            Boolean lifetime,
        @QueryParam("type")
            String type,
        @QueryParam("name")
            String name,
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
    List<QualifiedName> searchByOwners(
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
    void deleteDefinitionMetadata(
        @QueryParam("name")
            String name,
        @DefaultValue("false")
        @QueryParam("force")
            Boolean force
    );
}
