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
import java.util.Set;

/**
 * APIs to manipulate the tags.
 *
 * @author amajumdar
 */
@Path("mds/v1/tag")
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
    List<QualifiedName> list(
        @QueryParam("include")
            Set<String> includeTags,
        @QueryParam("exclude")
            Set<String> excludeTags,
        @QueryParam("sourceName")
            String sourceName,
        @QueryParam("databaseName")
            String databaseName,
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
    List<QualifiedName> search(
        @QueryParam("tag")
            String tag,
        @QueryParam("sourceName")
            String sourceName,
        @QueryParam("databaseName")
            String databaseName,
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
    Set<String> setTableTags(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
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
    void removeTableTags(
        @PathParam("catalog-name")
            String catalogName,
        @PathParam("database-name")
            String databaseName,
        @PathParam("table-name")
            String tableName,
        @DefaultValue("false")
        @QueryParam("all")
            Boolean deleteAll,
            Set<String> tags
    );
}
