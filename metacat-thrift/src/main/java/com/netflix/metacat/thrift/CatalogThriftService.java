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
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServerEventHandler;

/**
 * Thrift service.
 */
public class CatalogThriftService extends AbstractThriftServer {
    private final String catalogName;
    private final HiveConverters hiveConverters;
    private final MetacatV1 metacatV1;
    private final PartitionV1 partitionV1;
    private final TypeConverterProvider typeConverterProvider;

    /**
     * Constructor.
     * @param config config
     * @param typeConverterProvider coverter
     * @param hiveConverters hive converter
     * @param metacatV1 Metacat V1 resource
     * @param partitionV1 Partition V1 resource
     * @param catalogName catalog name
     * @param portNumber port
     */
    public CatalogThriftService(final Config config, final TypeConverterProvider typeConverterProvider,
        final HiveConverters hiveConverters, final MetacatV1 metacatV1, final PartitionV1 partitionV1,
        final String catalogName, final int portNumber) {
        super(config, portNumber, "thrift-pool-" + catalogName + "-" + portNumber + "-%d");
        this.hiveConverters = hiveConverters;
        this.typeConverterProvider = typeConverterProvider;
        this.metacatV1 = metacatV1;
        this.partitionV1 = partitionV1;
        this.catalogName = catalogName;
    }

    @Override
    public TProcessor getProcessor() {
        return new ThriftHiveMetastore.Processor<>(
            new CatalogThriftHiveMetastore(config, typeConverterProvider, hiveConverters, metacatV1, partitionV1,
                catalogName));
    }

    @Override
    public TServerEventHandler getServerEventHandler() {
        return new CatalogThriftEventHandler();
    }

    @Override
    public String getServerName() {
        return "thrift server for " + catalogName + " on port " + portNumber;
    }

    @Override
    public boolean hasServerEventHandler() {
        return true;
    }
}
