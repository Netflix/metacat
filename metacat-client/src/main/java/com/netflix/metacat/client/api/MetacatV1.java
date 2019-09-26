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

import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;

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
import java.util.List;

/**
 * Metacat API for managing catalog/database/table/mview.
 *
 * @author amajumdar
 */
@Path("mds/v1")
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
    void createDatabase(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
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
    TableDto createTable(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
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
    TableDto createMView(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
        @DefaultValue("false")
        @QueryParam("snapshot")
            Boolean snapshot,
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
    void deleteDatabase(
        @PathParam("catalog-name")
            String catalogName,
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
    TableDto deleteTable(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
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
    TableDto deleteMView(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
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
    CatalogDto getCatalog(
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
    List<CatalogMappingDto> getCatalogNames();

    /**
     * Get the database with the list of table names under it.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param includeUserMetadata true if details should include user metadata
     * @param includeTableNames   if true, include the list of table names
     * @return database with details
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    DatabaseDto getDatabase(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @DefaultValue("true")
        @QueryParam("includeUserMetadata")
            Boolean includeUserMetadata,
        @DefaultValue("true")
        @QueryParam("includeTableNames")
            Boolean includeTableNames
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
    default TableDto getTable(
            String catalogName,
            String databaseName,
            String tableName,
            Boolean includeInfo,
            Boolean includeDefinitionMetadata,
            Boolean includeDataMetadata
    ) {
        return getTable(catalogName, databaseName, tableName, includeInfo,
            includeDefinitionMetadata, includeDataMetadata, false);
    }

    /**
     * Get the table.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     * @param includeInfo               true if the details need to be included
     * @param includeDefinitionMetadata true if the definition metadata to be included
     * @param includeDataMetadata       true if the data metadata to be included
     * @param includeInfoDetails        true if the more info details to be included
     * @return table
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    TableDto getTable(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @DefaultValue("true")
        @QueryParam("includeInfo")
            Boolean includeInfo,
        @DefaultValue("true")
        @QueryParam("includeDefinitionMetadata")
            Boolean includeDefinitionMetadata,
        @DefaultValue("true")
        @QueryParam("includeDataMetadata")
            Boolean includeDataMetadata,
        @DefaultValue("false")
        @QueryParam("includeInfoDetails")
            Boolean includeInfoDetails
    );

    /**
     * Returns a filtered list of table names.
     * @param catalogName  catalog name
     * @param filter       filter expression
     * @param limit        list size
     * @return list of table names
     */
    @GET
    @Path("catalog/{catalog-name}/table-names")
    @Produces(MediaType.APPLICATION_JSON)
    List<QualifiedName> getTableNames(
        @PathParam("catalog-name")
            final String catalogName,
        @QueryParam("filter")
            final String filter,
        @QueryParam("limit")
            Integer limit
    );

    /**
     * Returns a filtered list of table names.
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param filter       filter expression
     * @param limit        list size
     * @return list of table names
     */
    @GET
    @Path("catalog/{catalog-name}/database/{database-name}/table-names")
    @Produces(MediaType.APPLICATION_JSON)
    List<QualifiedName> getTableNames(
        @PathParam("catalog-name")
        final String catalogName,
        @PathParam("database-name")
        final String databaseName,
        @QueryParam("filter")
        final String filter,
        @QueryParam("limit")
            Integer limit
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
    List<NameDateDto> getMViews(
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
    List<NameDateDto> getMViews(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
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
    TableDto getMView(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
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
    void renameTable(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
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
    void updateCatalog(
        @PathParam("catalog-name")
            String catalogName,
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
    void updateDatabase(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
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
    TableDto updateMView(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @PathParam("view-name")
            String viewName,
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
    TableDto updateTable(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        TableDto table
    );
}
