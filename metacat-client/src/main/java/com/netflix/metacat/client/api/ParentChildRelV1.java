package com.netflix.metacat.client.api;

import javax.ws.rs.Path;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.PathParam;

import javax.ws.rs.core.MediaType;
import com.netflix.metacat.common.dto.notifications.ChildInfoDto;

import java.util.Set;

/**
 * Metacat API for managing parent child relation.
 *
 * @author Yingjianw
 */

@Path("/mds/v1/parentChildRel")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface ParentChildRelV1 {
    /**
     * Return the list of children.
     * @param catalogName catalogName
     * @param databaseName databaseName
     * @param tableName tableName
     * @return list of childInfos
     */
    @GET
    @Path("children/catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Set<ChildInfoDto> getChildren(
        @PathParam("catalog-name")
        String catalogName,
        @PathParam("database-name")
        String databaseName,
        @PathParam("table-name")
        String tableName
    );
}
