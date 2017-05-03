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

import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Metacat API for managing catalog/database/table/mview.
 *
 * @author amajumdar
 */
//TODO: Don't think this kind of API definition should be in common
@Path("mds/v1")
@Api(
    value = "MetacatV1",
    description = "Federated metadata operations",
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON
)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface MetacatV1 {
    /**
     * Creates a new catalog.
     *
     * @param createCatalogDto catalog
     */
    @POST
    @Path("catalog")
    @ApiOperation(
        position = 3,
        value = "Creates a new catalog",
        notes = "Returns success if there were no errors creating the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "No catalogs are registered with the server"
            )
        }
    )
    void createCatalog(CreateCatalogDto createCatalogDto);

    /**
     * Creates the given database in the given catalog.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param databaseCreateRequestDto database create request
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 2,
        value = "Creates the given database in the given catalog",
        notes = "Given a catalog and a database name, creates the database in the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database cannot be located"
            )
        }
    )
    void createDatabase(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The database information")
            DatabaseCreateRequestDto databaseCreateRequestDto
    );

    /**
     * Creates a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        TableDto with table details
     * @return created <code>TableDto</code> table
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 2,
        value = "Creates a table",
        notes = "Creates the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto createTable(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The table information", required = true)
            TableDto table
    );

    /**
     * Creates a metacat view. A staging table that can contain partitions referring to the table partition locations.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param snapshot     boolean to snapshot or not
     * @param filter       filter expression to use
     * @return created <code>TableDto</code> mview
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 2,
        value = "Creates a metacat view. A staging table that can contain partitions referring to the table partition "
            + "locations.",
        notes = "Creates the given metacat view. A staging table that can contain partitions referring to the table "
            + "partition locations."
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto createMView(
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
        @ApiParam(
            value = "To snapshot a list of partitions of the table to this view. "
                + "If true, it will restore the partitions from the table to this view."
        )
        @DefaultValue("false")
        @QueryParam("snapshot")
            Boolean snapshot,
        @ApiParam(value = "Filter expression string to use")
        @QueryParam("filter")
            String filter
    );

    /**
     * Deletes the given database from the given catalog.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 4,
        value = "Deletes the given database from the given catalog",
        notes = "Given a catalog and database, deletes the database from the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database cannot be located"
            )
        }
    )
    void deleteDatabase(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName
    );

    /**
     * Delete table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return deleted <code>TableDto</code> table.
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 4,
        value = "Delete table",
        notes = "Deletes the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto deleteTable(
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
     * Delete metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return deleted <code>TableDto</code> mview.
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 4,
        value = "Delete metacat view",
        notes = "Deletes the given metacat view"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    TableDto deleteMView(
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
            String viewName
    );

    /**
     * Get the catalog by name.
     *
     * @param catalogName catalog name
     * @return catalog
     */
    @GET
    @Path("catalog/{catalog-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 2,
        value = "Databases for the requested catalog",
        notes = "The list of databases that belong to the given catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog cannot be located"
            )
        }
    )
    CatalogDto getCatalog(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName
    );

    /**
     * List registered catalogs.
     *
     * @return registered catalogs.
     */
    @GET
    @Path("catalog")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "List registered catalogs",
        notes = "The names and types of all catalogs registered with this server"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "No catalogs are registered with the server"
            )
        }
    )
    List<CatalogMappingDto> getCatalogNames();

    /**
     * Get the database with the list of table names under it.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param includeUserMetadata true if details should include user metadata
     * @return database with details
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Tables for the requested database",
        notes = "The list of tables that belong to the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database cannot be located"
            )
        }
    )
    DatabaseDto getDatabase(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "Whether to include user metadata information to the response")
        @DefaultValue("true")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata
    );

    /**
     * Get the table.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     * @param includeInfo               true if the details need to be included
     * @param includeDefinitionMetadata true if the definition metadata to be included
     * @param includeDataMetadata       true if the data metadata to be included
     * @return table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Table information",
        notes = "Table information for the given table name under the given catalog and database")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto getTable(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Whether to include the core information about the table (location, serde, columns) in "
            + "the response. You would only say false here if you only want metadata.")
        @DefaultValue("true")
        @QueryParam("includeInfo")
            Boolean includeInfo,
        @ApiParam(value = "Whether to include user definition metadata information to the response")
        @DefaultValue("true")
        @QueryParam("includeDefinitionMetadata")
            Boolean includeDefinitionMetadata,
        @ApiParam(value = "Whether to include user data metadata information to the response")
        @DefaultValue("true")
        @QueryParam("includeDataMetadata")
            Boolean includeDataMetadata
    );

    /**
     * List of metacat view names.
     *
     * @param catalogName catalog name
     * @return list of metacat view names.
     */
    @GET
    @Path("catalog/{catalog-name}/mviews")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "List of metacat views",
        notes = "List of metacat views for a catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog cannot be located"
            )
        }
    )
    List<NameDateDto> getMViews(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName
    );

    /**
     * List of metacat view names.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return List of metacat view names.
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mviews")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "List of metacat views",
        notes = "List of metacat views for a catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog cannot be located"
            )
        }
    )
    List<NameDateDto> getMViews(
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
     * Get metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return metacat view
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Metacat View information",
        notes = "View information for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto getMView(
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

    /**
     * Rename table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param newTableName new table name
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/rename")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 3,
        value = "Rename table",
        notes = "Renames the given table with the new name")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    void renameTable(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The name of the table", required = true)
        @QueryParam("newTableName")
            String newTableName
    );

    /**
     * Updates an existing catalog.
     *
     * @param catalogName      catalog name
     * @param createCatalogDto catalog
     */
    @PUT
    @Path("catalog/{catalog-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 4,
        value = "Updates an existing catalog",
        notes = "Returns success if there were no errors updating the catalog")
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "No catalogs are registered with the server"
            )
        }
    )
    void updateCatalog(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The metadata to update in the catalog", required = true)
            CreateCatalogDto createCatalogDto
    );

    /**
     * Updates the given database in the given catalog.
     *
     * @param catalogName              catalog name.
     * @param databaseName             database name.
     * @param databaseUpdateRequestDto database
     */
    @PUT
    @Path("catalog/{catalog-name}/database/{database-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 3,
        value = "Updates the given database in the given catalog",
        notes = "Given a catalog and a database name, creates the database in the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database cannot be located"
            )
        }
    )
    void updateDatabase(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The database information")
            DatabaseCreateRequestDto databaseUpdateRequestDto
    );

    /**
     * Update metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param table        view
     * @return updated metacat view
     */
    @PUT
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 3,
        value = "Update mview",
        notes = "Updates the given mview"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto updateMView(
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
        @ApiParam(value = "The view information", required = true)
            TableDto table
    );

    /**
     * Update table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        table
     * @return table
     */
    @PUT
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 3,
        value = "Update table",
        notes = "Updates the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    TableDto updateTable(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "The table information", required = true)
            TableDto table
    );
}
