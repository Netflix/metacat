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
import java.util.Set;

/**
 * APIs to manipulate the tags.
 *
 * @author amajumdar
 */
@Path("v1/tag")
@Api(value = "TagV1",
    description = "Federated metadata tag operations",
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface TagV1 {
    /**
     * Return the list of tags.
     *
     * @return list of tags
     */
    @GET
    @Path("tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Returns the tags",
        notes = "Returns the tags")
    Set<String> getTags();

    /**
     * Returns the list of qualified names for the given input.
     *
     * @param includeTags  Set of matching tags
     * @param excludeTags  Set of un-matching tags
     * @param sourceName   Prefix of the source name
     * @param databaseName Prefix of the database name
     * @param tableName    Prefix of the table name
     * @return list of qualified names
     */
    @GET
    @Path("list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Returns the list of qualified names that are tagged with the given tags."
            + " Qualified names will be excluded if the contained tags matches the excluded tags",
        notes = "Returns the list of qualified names that are tagged with the given tags."
            + " Qualified names will be excluded if the contained tags matches the excluded tags")
    List<QualifiedName> list(
        @ApiParam(value = "Set of matching tags")
        @QueryParam("include")
            Set<String> includeTags,
        @ApiParam(value = "Set of un-matching tags")
        @QueryParam("exclude")
            Set<String> excludeTags,
        @ApiParam(value = "Prefix of the source name")
        @QueryParam("sourceName")
            String sourceName,
        @ApiParam(value = "Prefix of the database name")
        @QueryParam("databaseName")
            String databaseName,
        @ApiParam(value = "Prefix of the table name")
        @QueryParam("tableName")
            String tableName
    );

    /**
     * Returns the list of qualified names that are tagged with tags containing the given tagText.
     *
     * @param tag          Tag partial text
     * @param sourceName   Prefix of the source name
     * @param databaseName Prefix of the database name
     * @param tableName    Prefix of the table name
     * @return list of qualified names
     */
    @GET
    @Path("search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 1,
        value = "Returns the list of qualified names that are tagged with tags containing the given tagText",
        notes = "Returns the list of qualified names that are tagged with tags containing the given tagText")
    List<QualifiedName> search(
        @ApiParam(value = "Tag partial text")
        @QueryParam("tag")
            String tag,
        @ApiParam(value = "Prefix of the source name")
        @QueryParam("sourceName")
            String sourceName,
        @ApiParam(value = "Prefix of the database name")
        @QueryParam("databaseName")
            String databaseName,
        @ApiParam(value = "Prefix of the table name")
        @QueryParam("tableName")
            String tableName
    );

    /**
     * Sets the tags on the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param tags         set of tags
     * @return set of tags
     */
    @POST
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 2,
        value = "Sets the tags on the given table",
        notes = "Sets the tags on the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    Set<String> setTableTags(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "Set of tags", required = true)
            Set<String> tags
    );

    /**
     * Remove the tags from the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param deleteAll    True if all tags need to be removed
     * @param tags         Tags to be removed from the given table
     */
    @DELETE
    @Path("catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        position = 4,
        value = "Remove the tags from the given table",
        notes = "Remove the tags from the given table")
    @ApiResponses(value = {
        @ApiResponse(code = HttpURLConnection.HTTP_NOT_FOUND,
            message = "The requested catalog or database or table cannot be located"
        )
    })
    void removeTableTags(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathParam("catalog-name")
            String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathParam("database-name")
            String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathParam("table-name")
            String tableName,
        @ApiParam(value = "True if all tags need to be removed")
        @DefaultValue("false")
        @QueryParam("all")
            Boolean deleteAll,
        @ApiParam(value = "Tags to be removed from the given table")
            Set<String> tags
    );
}
