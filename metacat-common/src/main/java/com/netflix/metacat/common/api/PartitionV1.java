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

import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.SortOrder;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

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
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Metacat API for managing partition.
 *
 * @author amajumdar
 */
@Path("v1/partition")
@Api(value = "PartitionV1",
    description = "Federated partition metadata operations",
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON)
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
    @ApiOperation(
        value = "Delete named partitions from a table",
        notes = "List of partitions names of the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        ),
        @ApiResponse(code = HttpURLConnection.HTTP_BAD_REQUEST,
            message = "The list of partitionNames is not present"
        )
    })
    void deletePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "partitionId of the partitions to be deleted from this table", required = true)
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
    @ApiOperation(
        value = "Delete partitions for the given view",
        notes = "Delete partitions for the given view")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        ),
        @ApiResponse(code = HttpURLConnection.HTTP_BAD_REQUEST,
            message = "The list of partitionNames is not present"
        )
    })
    void deletePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "partitionId of the partitions to be deleted from this table", required = true)
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
    @ApiOperation(
        value = "List of partitions for a table",
        notes = "List of partitions for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<PartitionDto> getPartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
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
    @ApiOperation(
        value = "List of partitions for a metacat view",
        notes = "List of partitions for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<PartitionDto> getPartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
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
    @ApiOperation(
        value = "List of partitions for a table",
        notes = "List of partitions for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<PartitionDto> getPartitionsForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        value = "List of partitions for a metacat view",
        notes = "List of partitions for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<PartitionDto> getPartitionsForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @DefaultValue("false")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        value = "List of partition keys for a table",
        notes = "List of partition keys for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<String> getPartitionKeys(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
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
    @ApiOperation(
        value = "List of partition keys for a metacat view",
        notes = "List of partition keys for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<String> getPartitionKeys(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
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
    @ApiOperation(
        value = "List of partition keys for a table",
        notes = "List of partition keys for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<String> getPartitionKeysForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        value = "List of partition keys for a metacat view",
        notes = "List of partition keys for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<String> getPartitionKeysForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        value = "List of partition uris for a table",
        notes = "List of partition uris for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<String> getPartitionUris(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
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
    @ApiOperation(
        value = "List of partition uris for a metacat view",
        notes = "List of partition uris for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<String> getPartitionUris(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
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
    @ApiOperation(
        value = "List of partition uris for a table",
        notes = "List of partition uris for the given table name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    List<String> getPartitionUrisForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        value = "List of partition uris for a metacat view",
        notes = "List of partition uris for the given view name under the given catalog and database")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or metacat view cannot be located"
        )
    })
    List<String> getPartitionUrisForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @QueryParam("sortBy")
            String sortBy,
        @ApiParam(value = "Sorting order to use")
        @QueryParam("sortOrder")
            SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @QueryParam("offset")
            Integer offset,
        @ApiParam(value = "Size of the partition list")
        @QueryParam("limit")
            Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
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
    @ApiOperation(
        position = 5,
        value = "Add/update partitions to the given table",
        notes = "Add/update partitions to the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    PartitionsSaveResponseDto savePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Request containing the list of partitions", required = true)
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
    @ApiOperation(
        position = 5,
        value = "Add/update partitions to the given table",
        notes = "Add/update partitions to the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    PartitionsSaveResponseDto savePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the view", required = true)
        @PathParam("view-name")
            String viewName,
        @ApiParam(value = "Request containing the list of partitions", required = true)
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
    @ApiOperation(
        position = 5,
        value = "Partition count for the given table",
        notes = "Partition count for the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    Integer getPartitionCount(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
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
    @ApiOperation(
        position = 5,
        value = "Partition count for the given table",
        notes = "Partition count for the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    Integer getPartitionCount(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the view", required = true)
        @PathParam("view-name")
            String viewName
    );
}
