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

package com.netflix.metacat.connector.druid;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory;
import com.netflix.metacat.connector.druid.configs.DruidConnectorConfig;
import com.netflix.metacat.connector.druid.configs.BaseDruidHttpClientConfig;
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter;

/**
 * Druid Connector Factory.
 *
 * @author zhenl jtuglu
 * @since 1.2.0
 */
public class DruidConnectorFactory extends SpringConnectorFactory {
    /**
     * Constructor.
     *
     * @param druidConnectorInfoConverter connector info converter
     * @param connectorContext            connector context
     * @param clientConfigClass           config class to register
     */
    public DruidConnectorFactory(
        final DruidConnectorInfoConverter druidConnectorInfoConverter,
        final ConnectorContext connectorContext,
        final Class<? extends BaseDruidHttpClientConfig> clientConfigClass
    ) {
        super(druidConnectorInfoConverter, connectorContext);
        super.registerClazz(DruidConnectorConfig.class, clientConfigClass);
        super.refresh();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.ctx.getBean(DruidConnectorDatabaseService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTableService getTableService() {
        return this.ctx.getBean(DruidConnectorTableService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.ctx.getBean(DruidConnectorPartitionService.class);
    }
}
