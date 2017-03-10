package com.netflix.metacat.connector.hive.embedded;

import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.thrift.TException;

import javax.inject.Inject;

/**
 * Embedded hive metastore client implementation.
 */
public class EmbeddedHiveClient implements IMetacatHiveClient {
    private final IHMSHandler handler;

    /**
     * Embedded hive client implementation.
     * @param handler hive metastore handler
     */
    @Inject
    public EmbeddedHiveClient(final IHMSHandler handler) {
        this.handler = handler;
    }

    @Override
    public void shutdown() throws TException {
        handler.shutdown();
    }
}
