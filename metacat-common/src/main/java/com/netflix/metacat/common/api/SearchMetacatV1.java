package com.netflix.metacat.common.api;

import com.netflix.metacat.common.dto.TableDto;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("mds/v1/search")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface SearchMetacatV1 {
    @GET
    @Path("table")
    @Consumes(MediaType.APPLICATION_JSON)
    List<TableDto> searchTables(
            @QueryParam("q")
            String searchString
    );
}
