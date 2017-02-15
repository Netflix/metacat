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

package com.netflix.metacat.thrift;

import com.google.common.base.Objects;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.util.MetacatContextManager;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Server event handler.
 */
public class CatalogThriftEventHandler implements TServerEventHandler {

    @Override
    public ServerContext createContext(final TProtocol input, final TProtocol output) {
        final String userName = "metacat-thrift-interface";
        String clientHost = null; //requestContext.getHeaderString("X-Forwarded-For");
        final long requestThreadId = Thread.currentThread().getId();

        final TTransport transport = input.getTransport();
        if (transport instanceof TSocket) {
            final TSocket thriftSocket = (TSocket) transport;
            clientHost = thriftSocket.getSocket().getInetAddress().getHostAddress();
        }

        final CatalogServerRequestContext context = new CatalogServerRequestContext(
            userName,
            null,
            clientHost,
            null,
            "hive",
            requestThreadId
        );
        MetacatContextManager.setContext(context);
        return context;
    }

    @Override
    public void deleteContext(final ServerContext serverContext, final TProtocol input, final TProtocol output) {
        validateRequest((CatalogServerRequestContext) serverContext);
        MetacatContextManager.removeContext();
    }

    @Override
    public void preServe() {
        // nothing to do
    }

    @Override
    public void processContext(final ServerContext serverContext, final TTransport inputTransport,
        final TTransport outputTransport) {
        validateRequest((CatalogServerRequestContext) serverContext);
    }

    private void validateRequest(final CatalogServerRequestContext serverContext) {
        final long requestThreadId = serverContext.requestThreadId;
        if (requestThreadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Expect all processing to happen on the same thread as the request thread");
        }
    }

    /**
     * request context.
     */
    public static class CatalogServerRequestContext extends MetacatRequestContext implements ServerContext {
        private final long requestThreadId;

        CatalogServerRequestContext(
            final String userName,
            final String clientAppName,
            final String clientId,
            final String jobId,
            final String dataTypeContext,
            final long requestThreadId
        ) {
            super(userName, clientAppName, clientId, jobId, dataTypeContext);
            this.requestThreadId = requestThreadId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            final CatalogServerRequestContext that = (CatalogServerRequestContext) o;
            return requestThreadId == that.requestThreadId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), requestThreadId);
        }
    }
}
