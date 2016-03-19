package com.netflix.metacat.common.api;

import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.SortOrder;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

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
 * Created by amajumdar on 6/17/15.
 */
@Path("mds/v1/partition")
@Api(value = "PartitionV1",
        description = "Federated partition metadata operations",
        produces = MediaType.APPLICATION_JSON,
        consumes = MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface PartitionV1 {
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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );

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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );

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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Filter expression string to use", required = false)
            @QueryParam("filter")
            String filter,
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );

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
            @ApiParam(value = "Sort the partition list by this value", required = false)
            @QueryParam("sortBy")
            String sortBy,
            @ApiParam(value = "Sorting order to use", required = false)
            @QueryParam("sortOrder")
            SortOrder sortOrder,
            @ApiParam(value = "Offset of the list returned", required = false)
            @QueryParam("offset")
            Integer offset,
            @ApiParam(value = "Size of the partition list", required = false)
            @QueryParam("limit")
            Integer limit,
            @ApiParam(value = "Whether to include user metadata information to the response", required = false)
            @DefaultValue("false") @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
            @ApiParam(value = "Request containing the filter expression for the partitions", required = false)
            GetPartitionsRequestDto getPartitionsRequestDto
    );


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
