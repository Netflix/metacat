package com.netflix.metacat.common.api;

import com.netflix.metacat.common.QualifiedName;
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
import java.util.Set;

@Path("mds/v1/tag")
@Api(value = "TagV1",
        description = "Federated metadata tag operations",
        produces = MediaType.APPLICATION_JSON,
        consumes = MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface TagV1 {
    @GET
    @Path("tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            position = 1,
            value = "Returns the tags",
            notes = "Returns the tags")
    Set<String> getTags();

    @GET
    @Path("list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            position = 1,
            value = "Returns the list of qualified names that are tagged with the given tags. Qualified names will be excluded if the contained tags matches the excluded tags",
            notes = "Returns the list of qualified names that are tagged with the given tags. Qualified names will be excluded if the contained tags matches the excluded tags")
    List<QualifiedName> list(
            @ApiParam(value = "Set of matching tags", required = false)
            @QueryParam("include")
            Set<String> includeTags,
            @ApiParam(value = "Set of un-matching tags", required = false)
            @QueryParam("exclude")
            Set<String> excludeTags,
            @ApiParam(value = "Prefix of the source name", required = false)
            @QueryParam("sourceName")
            String sourceName,
            @ApiParam(value = "Prefix of the database name", required = false)
            @QueryParam("databaseName")
            String databaseName,
            @ApiParam(value = "Prefix of the table name", required = false)
            @QueryParam("tableName")
            String tableName
    );

    @GET
    @Path("search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            position = 1,
            value = "Returns the list of qualified names that are tagged with tags containing the given tagText",
            notes = "Returns the list of qualified names that are tagged with tags containing the given tagText")
    List<QualifiedName> search(
            @ApiParam(value = "Tag partial text", required = false)
            @QueryParam("tag")
            String tag,
            @ApiParam(value = "Prefix of the source name", required = false)
            @QueryParam("sourceName")
            String sourceName,
            @ApiParam(value = "Prefix of the database name", required = false)
            @QueryParam("databaseName")
            String databaseName,
            @ApiParam(value = "Prefix of the table name", required = false)
            @QueryParam("tableName")
            String tableName
    );

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
            @ApiParam(value = "True if all tags need to be removed", required = false)
            @DefaultValue("false") @QueryParam("all")
            Boolean deleteAll,
            @ApiParam(value = "Tags to be removed from the given table", required = false)
            Set<String> tags
    );
}
