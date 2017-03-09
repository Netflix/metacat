/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.thrift;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;

/**
 * HiveMetastoreClient.
 *
 * @author zhenl
 */
public class HiveMetastoreClient
        extends ThriftHiveMetastore.Client
        implements Closeable {
    private final TTransport transport;

    /**
     * Constructor.
     *
     * @param transport transport
     */
    public HiveMetastoreClient(final TTransport transport) {
        super(new TBinaryProtocol(transport));
        this.transport = transport;
    }

    /**
     * Constructor.
     *
     * @param protocol protocol
     */
    public HiveMetastoreClient(final TProtocol protocol) {
        super(protocol);
        this.transport = protocol.getTransport();
    }

    @Override
    public void close() {
        transport.close();
    }
}
