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
package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.api.v1.PartitionV1;
import com.netflix.metacat.main.api.RequestWrapper;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;

/**
 * Partition V1 API implementation.
 *
 * @author amajumdar
 * @author zhenl
 */
@RestController
@RequestMapping(
    path = "/mds/v1/partition",
    produces = MediaType.APPLICATION_JSON_VALUE
)
@Api(value = "PartitionV1",
    description = "Federated partition metadata operations",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE
)
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PartitionController implements PartitionV1 {

    private final MetacatController v1;
    private final MViewService mViewService;
    private final PartitionService partitionService;
    private final RequestWrapper requestWrapper;

    /**
     * Delete named partitions from a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param partitionIds lis of partition names
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
        value = "Delete named partitions from a table",
        notes = "List of partitions names of the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were deleted successfully"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_BAD_REQUEST,
                message = "The list of partitionNames is not present"
            )
        }
    )
    @Override
    public void deletePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "partitionId of the partitions to be deleted from this table", required = true)
        @RequestBody final List<String> partitionIds
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        this.requestWrapper.processRequest(
            name,
            "deleteTablePartition",
            () -> {
                if (partitionIds.isEmpty()) {
                    throw new IllegalArgumentException("partitionIds are required");
                }
                this.partitionService.delete(name, partitionIds);
                return null;
            }
        );
    }

    /**
     * Delete partitions for the given view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     metacat view name
     * @param partitionIds list of partition names
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
        value = "Delete partitions for the given view",
        notes = "Delete partitions for the given view"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were deleted successfully"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_BAD_REQUEST,
                message = "The list of partitionNames is not present"
            )
        }
    )
    public void deletePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "partitionId of the partitions to be deleted from this table", required = true)
        @RequestBody final List<String> partitionIds
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        this.requestWrapper.processRequest(
            name,
            "deleteMViewPartition",
            () -> {
                if (partitionIds.isEmpty()) {
                    throw new IllegalArgumentException("partitionIds are required");
                }
                this.mViewService.deletePartitions(name, partitionIds);
                return null;
            }
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partitions for a table",
        notes = "List of partitions for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public List<PartitionDto> getPartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "false") final boolean includeUserMetadata
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitions",
            Collections.singletonMap("filterPassed", StringUtils.isEmpty(filter) ? "false" : "true"),
            () -> this.partitionService.list(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                includeUserMetadata,
                includeUserMetadata,
                new GetPartitionsRequestDto(filter, null, false, false)
            )
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partitions for a metacat view",
        notes = "List of partitions for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<PartitionDto> getPartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "false") final boolean includeUserMetadata
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitions",
            Collections.singletonMap("filterPassed", StringUtils.isEmpty(filter) ? "false" : "true"),
            () -> this.mViewService.listPartitions(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                includeUserMetadata,
                new GetPartitionsRequestDto(filter, null, false, true)
            )
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partitions for a table",
        notes = "List of partitions for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were retrieved"
            ),
            @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public List<PartitionDto> getPartitionsForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "false") final boolean includeUserMetadata,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {

        return this.getPartitions(
            catalogName,
            databaseName,
            tableName,
            sortBy,
            sortOrder,
            offset,
            limit,
            includeUserMetadata,
            getPartitionsRequestDto
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partitions for a metacat view",
        notes = "List of partitions for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<PartitionDto> getPartitionsForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "false") final boolean includeUserMetadata,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        return this.getPartitions(
            catalogName,
            databaseName,
            tableName,
            viewName,
            sortBy,
            sortOrder,
            offset,
            limit,
            includeUserMetadata,
            getPartitionsRequestDto
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/keys"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition keys for a table",
        notes = "List of partition keys for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions keys were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public List<String> getPartitionKeys(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit
    ) {
        return this._getPartitionKeys(
            catalogName,
            databaseName,
            tableName,
            sortBy,
            sortOrder,
            offset,
            limit,
            new GetPartitionsRequestDto(filter, null, false, false)
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/keys"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition keys for a metacat view",
        notes = "List of partition keys for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions keys were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<String> getPartitionKeys(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit
    ) {
        return this._getMViewPartitionKeys(
            catalogName,
            databaseName,
            tableName,
            viewName,
            sortBy,
            sortOrder,
            offset,
            limit,
            new GetPartitionsRequestDto(filter, null, false, true)
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/keys-request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition keys for a table",
        notes = "List of partition keys for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions keys were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public List<String> getPartitionKeysForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        return this._getPartitionKeys(
            catalogName,
            databaseName,
            tableName,
            sortBy,
            sortOrder,
            offset,
            limit,
            getPartitionsRequestDto
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/keys-request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition keys for a metacat view",
        notes = "List of partition keys for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions keys were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<String> getPartitionKeysForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        return this._getMViewPartitionKeys(
            catalogName,
            databaseName,
            tableName,
            viewName,
            sortBy,
            sortOrder,
            offset,
            limit,
            getPartitionsRequestDto
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/uris"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition uris for a table",
        notes = "List of partition uris for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions uris were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public List<String> getPartitionUris(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit
    ) {
        return this._getPartitionUris(
            catalogName,
            databaseName,
            tableName,
            sortBy,
            sortOrder,
            offset,
            limit,
            new GetPartitionsRequestDto(filter, null, false, false)
        );
    }

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
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/uris"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition uris for a metacat view",
        notes = "List of partition uris for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions uris were retrieved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<String> getPartitionUris(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Filter expression string to use")
        @Nullable @RequestParam(name = "filter", required = false) final String filter,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit
    ) {
        return this._getMViewPartitionUris(
            catalogName,
            databaseName,
            tableName,
            viewName,
            sortBy,
            sortOrder,
            offset,
            limit,
            new GetPartitionsRequestDto(filter, null, false, true)
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/uris-request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition uris for a table",
        notes = "List of partition uris for the given table name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions uris were retrieved"
            ),
            @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public List<String> getPartitionUrisForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        return this._getPartitionUris(
            catalogName,
            databaseName,
            tableName,
            sortBy,
            sortOrder,
            offset,
            limit,
            getPartitionsRequestDto
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/uris-request",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "List of partition uris for a metacat view",
        notes = "List of partition uris for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partitions uris were retrieved"
            ),
            @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public List<String> getPartitionUrisForRequest(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Sort the partition list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @ApiParam(value = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @ApiParam(value = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @ApiParam(value = "Size of the partition list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @ApiParam(value = "Request containing the filter expression for the partitions")
        @Nullable @RequestBody(required = false) final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        return this._getMViewPartitionUris(
            catalogName,
            databaseName,
            tableName,
            viewName,
            sortBy,
            sortOrder,
            offset,
            limit,
            getPartitionsRequestDto
        );
    }

    /**
     * Add/update partitions to the given table.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param tableName                table name
     * @param partitionsSaveRequestDto partition request containing the list of partitions to be added/updated
     * @return Response with the number of partitions added/updated
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        position = 5,
        value = "Add/update partitions to the given table",
        notes = "Add/update partitions to the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The partitions were successfully saved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public PartitionsSaveResponseDto savePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Request containing the list of partitions", required = true)
        @RequestBody final PartitionsSaveRequestDto partitionsSaveRequestDto
    ) {
        final QualifiedName name = QualifiedName.ofTable(catalogName, databaseName, tableName);
        return this.requestWrapper.processRequest(
            name,
            "saveTablePartition",
            () -> {
                final PartitionsSaveResponseDto result;
                if (partitionsSaveRequestDto.getPartitions() == null
                    || partitionsSaveRequestDto.getPartitions().isEmpty()) {
                    result = new PartitionsSaveResponseDto();
                } else {
                    result = this.partitionService.save(name, partitionsSaveRequestDto);

                    // This metadata is actually for the table, if it is present update that
                    if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                        || partitionsSaveRequestDto.getDataMetadata() != null) {
                        final TableDto dto = new TableDto();
                        dto.setName(name);
                        dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                        dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                        this.v1.updateTable(catalogName, databaseName, tableName, dto);
                    }
                }
                return result;
            }
        );
    }

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
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        position = 5,
        value = "Add/update partitions to the given table",
        notes = "Add/update partitions to the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The partitions were successfully saved"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public PartitionsSaveResponseDto savePartitions(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the view", required = true)
        @PathVariable("view-name") final String viewName,
        @ApiParam(value = "Request containing the list of partitions", required = true)
        @RequestBody final PartitionsSaveRequestDto partitionsSaveRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "saveMViewPartition",
            () -> {
                final PartitionsSaveResponseDto result;
                if (partitionsSaveRequestDto.getPartitions() == null
                    || partitionsSaveRequestDto.getPartitions().isEmpty()) {
                    result = new PartitionsSaveResponseDto();
                } else {
                    result = mViewService.savePartitions(name, partitionsSaveRequestDto, true);
                    // This metadata is actually for the view, if it is present update that
                    if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                        || partitionsSaveRequestDto.getDataMetadata() != null) {
                        final TableDto dto = new TableDto();
                        dto.setName(name);
                        dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                        dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                        this.v1.updateMView(catalogName, databaseName, tableName, viewName, dto);
                    }
                }
                return result;
            }
        );
    }

    /**
     * Get the partition count for the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return partition count for the given table
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/count"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 5,
        value = "Partition count for the given table",
        notes = "Partition count for the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partition count was returned successfully"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public Integer getPartitionCount(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitionCount",
            () -> this.partitionService.count(name)
        );
    }

    /**
     * Get the partition count for the given metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return partition count for the given view
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}/count"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 5,
        value = "Partition count for the given table",
        notes = "Partition count for the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_OK,
                message = "The partition count was returned successfully"
            ),
            @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public Integer getPartitionCount(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "The name of the view", required = true)
        @PathVariable("view-name") final String viewName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitionCount",
            () -> this.mViewService.partitionCount(name)
        );
    }

    private List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        final boolean includeUserMetadata,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );

        return this.requestWrapper.processRequest(
            name,
            "getPartitions",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> partitionService.list(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                includeUserMetadata,
                includeUserMetadata,
                getPartitionsRequestDto
            )
        );
    }

    private List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        final boolean includeUserMetadata,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitions",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> this.mViewService.listPartitions(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                includeUserMetadata,
                getPartitionsRequestDto
            )
        );
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getPartitionUris(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitionUris",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> this.partitionService.getPartitionUris(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                getPartitionsRequestDto
            )
        );
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getMViewPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {

        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getMViewPartitionKeys",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> this.mViewService.getPartitionKeys(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                getPartitionsRequestDto
            )
        );
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getMViewPartitionUris(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getMViewPartitionUris",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> this.mViewService.getPartitionUris(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                getPartitionsRequestDto
            )
        );
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {

        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getPartitionKeys",
            Collections.singletonMap("filterPassed",
                getPartitionsRequestDto == null || StringUtils.isEmpty(
                    getPartitionsRequestDto.getFilter()) ? "false" : "true"),
            () -> partitionService.getPartitionKeys(
                name,
                new Sort(sortBy, sortOrder),
                new Pageable(limit, offset),
                getPartitionsRequestDto
            )
        );
    }

}
