package com.netflix.metacat.thrift;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.util.MetacatContextManager;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogThriftEventHandler implements TServerEventHandler {
    private static final Logger log = LoggerFactory.getLogger(CatalogThriftEventHandler.class);

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
        String userName = "metacat-thrift-interface";
        String clientAppName = null; //requestContext.getHeaderString(MetacatContext.HEADER_KEY_CLIENT_APP_NAME);
        String clientHost = null; //requestContext.getHeaderString("X-Forwarded-For");
        String jobId = null; //requestContext.getHeaderString(MetacatContext.HEADER_KEY_JOB_ID);
        String dataTypeContext = MetacatContext.DATA_TYPE_CONTEXTS.hive.name();
        long requestThreadId = Thread.currentThread().getId();

        TTransport transport = input.getTransport();
        if (transport instanceof TSocket) {
            TSocket thriftSocket = (TSocket) transport;
            clientHost = thriftSocket.getSocket().getInetAddress().getHostAddress();
        }

        CatalogServerContext context = new CatalogServerContext(userName, clientAppName, clientHost, jobId,
                dataTypeContext, requestThreadId);
        MetacatContextManager.setContext(context);
        return context;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
        validateRequest((CatalogServerContext) serverContext);
        MetacatContextManager.removeContext();
    }

    @Override
    public void preServe() {
        // nothing to do
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
        validateRequest((CatalogServerContext) serverContext);
    }

    protected void validateRequest(CatalogServerContext serverContext) {
        long requestThreadId = serverContext.requestThreadId;
        if (requestThreadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Expect all processing to happen on the same thread as the request thread");
        }
    }

    public static class CatalogServerContext extends MetacatContext implements ServerContext {
        public final long requestThreadId;

        public CatalogServerContext(String userName, String clientAppName, String clientId, String jobId,
                String dataTypeContext, long requestThreadId) {
            super(userName, clientAppName, clientId, jobId, dataTypeContext);
            this.requestThreadId = requestThreadId;
        }
    }
}
