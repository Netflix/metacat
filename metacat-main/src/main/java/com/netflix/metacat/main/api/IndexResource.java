package com.netflix.metacat.main.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("/")
public class IndexResource {
    @GET
    public Response index() {
        return Response.ok().build();
    }
}
