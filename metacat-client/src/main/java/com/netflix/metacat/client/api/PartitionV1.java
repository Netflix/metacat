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

import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.SortOrder;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Metacat API for managing partition.
 *
 * @author amajumdar
 */
@Path("mds/v1/partition")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface PartitionV1 {
    /**
     * Delete named partitions from a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param partitionIds lis of partition names
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    void deletePartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        List<String> partitionIds
    );

    /**
     * Delete partitions for the given view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     metacat view name
     * @param partitionIds list of partition names
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    void deletePartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        List<String> partitionIds
    );

    /**
     * Return list of partitions for a table.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param tableName           table name
     * @param filter              filter expression
     * @param sortBy              sort by this name
     * @param sortOrder           sort order to use
     * @param offset              offset of the list
     * @param limit               size of the list
     * @param includeUserMetadata whether to include user metadata for every partition in the list
     * @return list of partitions for a table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<PartitionDto> getPartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

    /**
     * Return list of partitions for a metacat view.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param tableName           table name
     * @param viewName            view name
     * @param filter              filter expression
     * @param sortBy              sort by this name
     * @param sortOrder           sort order to use
     * @param offset              offset of the list
     * @param limit               size of the list
     * @param includeUserMetadata whether to include user metadata for every partition in the list
     * @return list of partitions for a metacat view
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<PartitionDto> getPartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

    /**
     * Return list of partitions for a table.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param includeUserMetadata     whether to include user metadata for every partition in the list
     * @param getPartitionsRequestDto request
     * @return list of partitions for a table
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<PartitionDto> getPartitionsForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partitions for a view.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param viewName                view name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param includeUserMetadata     whether to include user metadata for every partition in the list
     * @param getPartitionsRequestDto request
     * @return list of partitions for a view
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<PartitionDto> getPartitionsForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partition names for a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param filter       filter expression
     * @param sortBy       sort by this name
     * @param sortOrder    sort order to use
     * @param offset       offset of the list
     * @param limit        size of the list
     * @return list of partition names for a table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/keys")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionKeys(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit
    );

    /**
     * Return list of partition names for a view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param filter       filter expression
     * @param sortBy       sort by this name
     * @param sortOrder    sort order to use
     * @param offset       offset of the list
     * @param limit        size of the list
     * @return list of partition names for a view
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/keys")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionKeys(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit
    );

    /**
     * Return list of partition names for a table.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param getPartitionsRequestDto request
     * @return list of partition names for a table
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/keys-request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionKeysForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partition names for a view.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param viewName                view name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param getPartitionsRequestDto request
     * @return list of partition names for a view
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/keys-request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionKeysForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partition uris for a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param filter       filter expression
     * @param sortBy       sort by this name
     * @param sortOrder    sort order to use
     * @param offset       offset of the list
     * @param limit        size of the list
     * @return list of partition uris for a table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/uris")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionUris(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit
    );

    /**
     * Return list of partition uris for a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param filter       filter expression
     * @param sortBy       sort by this name
     * @param sortOrder    sort order to use
     * @param offset       offset of the list
     * @param limit        size of the list
     * @return list of partition uris for a table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/uris")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionUris(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("filter")
            String filter,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit
    );

    /**
     * Return list of partition uris for a table.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param getPartitionsRequestDto request
     * @return list of partition uris for a table
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/uris-request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionUrisForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partition uris for a view.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param viewName                view name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param getPartitionsRequestDto request
     * @return list of partition uris for a view
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/uris-request")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    List<String> getPartitionUrisForRequest(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @QueryParam("sortBy")
            String sortBy,
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @QueryParam("offset")
            Integer offset,
        @QueryParam("limit")
            Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Add/update partitions to the given table.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param tableName                table name
     * @param partitionsSaveRequestDto partition request containing the list of partitions to be added/updated
     * @return Response with the number of partitions added/updated
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    PartitionsSaveResponseDto savePartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        PartitionsSaveRequestDto partitionsSaveRequestDto
    );

    /**
     * Add/update partitions to the given metacat view.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param tableName                table name
     * @param viewName                 view name
     * @param partitionsSaveRequestDto partition request containing the list of partitions to be added/updated
     * @return Response with the number of partitions added/updated
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    PartitionsSaveResponseDto savePartitions(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        PartitionsSaveRequestDto partitionsSaveRequestDto
    );

    /**
     * Get the partition count for the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return partition count for the given table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/count")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Integer getPartitionCount(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName
    );

    /**
     * Get the partition count for the given metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return partition count for the given view
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/count")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Integer getPartitionCount(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName
    );
}
