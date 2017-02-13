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

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.util.MetacatContextManager;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * REST filter.
 */
@Provider
@Slf4j
public class MetacatRestFilter implements ContainerRequestFilter, ContainerResponseFilter {

    @Override
    public void filter(final ContainerRequestContext requestContext) throws IOException {
        String userName = requestContext.getHeaderString(MetacatRequestContext.HEADER_KEY_USER_NAME);
        if (userName == null) {
            userName = "metacat";
        }
        final String clientAppName = requestContext.getHeaderString(MetacatRequestContext.HEADER_KEY_CLIENT_APP_NAME);
        final String clientHost = requestContext.getHeaderString("X-Forwarded-For");
        final String jobId = requestContext.getHeaderString(MetacatRequestContext.HEADER_KEY_JOB_ID);
        final String dataTypeContext =
            requestContext.getHeaderString(MetacatRequestContext.HEADER_KEY_DATA_TYPE_CONTEXT);
        final MetacatRequestContext context = new MetacatRequestContext(userName, clientAppName, clientHost, jobId,
            dataTypeContext);
        MetacatContextManager.setContext(context);
        log.info(context.toString());
    }

    @Override
    public void filter(final ContainerRequestContext requestContext, final ContainerResponseContext responseContext)
        throws IOException {
        MetacatContextManager.removeContext();
    }
}
