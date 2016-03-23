/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.server.jersey;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.util.MetacatContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Created by amajumdar on 8/3/15.
 */
@Provider
public class MetacatRestFilter implements ContainerRequestFilter, ContainerResponseFilter{
    private static final Logger log = LoggerFactory.getLogger(MetacatRestFilter.class);
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String userName = requestContext.getHeaderString(MetacatContext.HEADER_KEY_USER_NAME);
        if( userName == null){
            userName = "metacat";
        }
        String clientAppName = requestContext.getHeaderString(MetacatContext.HEADER_KEY_CLIENT_APP_NAME);
        String clientHost = requestContext.getHeaderString("X-Forwarded-For");
        String jobId = requestContext.getHeaderString(MetacatContext.HEADER_KEY_JOB_ID);
        String dataTypeContext = requestContext.getHeaderString(MetacatContext.HEADER_KEY_DATA_TYPE_CONTEXT);
        MetacatContext context = new MetacatContext( userName, clientAppName, clientHost, jobId, dataTypeContext);
        MetacatContextManager.setContext(context);
        log.info(context.toString());
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        MetacatContextManager.removeContext();
    }
}
