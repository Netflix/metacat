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

import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.converters.HiveConverters;
import com.netflix.metacat.converters.TypeConverterProvider;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogThriftService {
    private static final Logger log = LoggerFactory.getLogger(CatalogThriftService.class);
    private final String catalogName;
    private final Config config;
    private final HiveConverters hiveConverters;
    private final MetacatV1 metacatV1;
    private final PartitionV1 partitionV1;
    private final int portNumber;
    private final TypeConverterProvider typeConverterProvider;
    private TServer server;

    public CatalogThriftService(Config config, TypeConverterProvider typeConverterProvider,
            HiveConverters hiveConverters, MetacatV1 metacatV1, PartitionV1 partitionV1,
            String catalogName, int portNumber) {
        this.config = config;
        this.hiveConverters = hiveConverters;
        this.typeConverterProvider = typeConverterProvider;
        this.metacatV1 = metacatV1;
        this.partitionV1 = partitionV1;
        this.catalogName = catalogName;
        this.portNumber = portNumber;
    }

    public void start() throws Exception {
        log.info("initializing thrift server for {} on port {}", catalogName, portNumber);
        CatalogThriftHiveMetastore handler = new CatalogThriftHiveMetastore(config, typeConverterProvider,
                hiveConverters, metacatV1, partitionV1, catalogName);
        ThriftHiveMetastore.Processor<CatalogThriftHiveMetastore> processor = new ThriftHiveMetastore.Processor<>(
                handler);

        TServerTransport serverTransport = new TServerSocket(portNumber);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor);
        server = new TThreadPoolServer(serverArgs);
        server.setServerEventHandler(new CatalogThriftEventHandler());

        new Thread("thrift server for " + catalogName + ":" + portNumber) {
            @Override
            public void run() {
                log.debug("starting serving");
                server.serve();
                log.debug("started serving");
            }
        }.start();
    }

    public void stop() throws Exception {
        log.info("stopping thrift server for {} on port {}", catalogName, portNumber);
        if (server != null) {
            log.debug("stopping serving");
            server.stop();
            log.debug("stopped serving");
        }
    }
}
