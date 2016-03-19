package com.netflix.metacat.server.init;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.netflix.metacat.common.json.MetacatJsonLocator;

import javax.annotation.Priority;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

@Provider
@Produces(MediaType.APPLICATION_JSON)
@Priority(10_000)
public class MetacatJsonProvider extends JacksonJaxbJsonProvider {
    public MetacatJsonProvider() {
        super();
        setMapper(MetacatJsonLocator.INSTANCE.getObjectMapper());
    }
}
