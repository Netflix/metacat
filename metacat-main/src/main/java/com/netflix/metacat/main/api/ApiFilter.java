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

package com.netflix.metacat.main.api;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * REST Interceptor.
 *
 * @author amajumdar
 * @author tgianos
 * @since 1.1.0
 */
@Slf4j
public class ApiFilter implements Filter {

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doFilter(final ServletRequest request,
                         final ServletResponse response,
                         final FilterChain chain) throws IOException, ServletException {
        preFilter(request, response, chain);
        try {
            chain.doFilter(request, response);
        } finally {
            postFilter(request, response, chain);
        }
    }

    protected void preFilter(final ServletRequest request,
                             final ServletResponse response,
                             final FilterChain chain) throws ServletException {
        // Pre-processing
        if (!(request instanceof HttpServletRequest)) {
            throw new ServletException("Expected an HttpServletRequest but didn't get one");
        }

        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;

        String userName = httpServletRequest.getHeader(MetacatRequestContext.HEADER_KEY_USER_NAME);
        if (userName == null) {
            userName = "metacat";
        }

        final String clientAppName = httpServletRequest.getHeader(MetacatRequestContext.HEADER_KEY_CLIENT_APP_NAME);
        final String clientId = httpServletRequest.getHeader("X-Forwarded-For");
        final String jobId = httpServletRequest.getHeader(MetacatRequestContext.HEADER_KEY_JOB_ID);
        final String dataTypeContext = httpServletRequest.getHeader(MetacatRequestContext.HEADER_KEY_DATA_TYPE_CONTEXT);

        final MetacatRequestContext context = buildRequestContext(
            userName, clientAppName, clientId, jobId, dataTypeContext,
            httpServletRequest.getScheme(),
            httpServletRequest.getRequestURI(),
            httpServletRequest
        );

        MetacatContextManager.setContext(context);
        log.info(context.toString());
    }

    protected MetacatRequestContext buildRequestContext(final String userName,
                                                        final String clientAppName,
                                                        final String clientId,
                                                        final String jobId,
                                                        final String dataTypeContext,
                                                        final String scheme,
                                                        final String requestUri,
                                                        final HttpServletRequest httpServletRequest) {
        return MetacatRequestContext.builder()
                   .userName(userName)
                   .clientAppName(clientAppName)
                   .clientId(clientId)
                   .jobId(jobId)
                   .dataTypeContext(dataTypeContext)
                   .scheme(scheme)
                   .apiUri(requestUri)
                   .build();
    }

    protected void postFilter(final ServletRequest request,
                              final ServletResponse response,
                              final FilterChain chain) throws ServletException {
        MetacatContextManager.removeContext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
    }
}
