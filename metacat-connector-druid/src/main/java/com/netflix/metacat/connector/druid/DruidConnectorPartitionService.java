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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.druid.converter.DataSource;
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter;
import com.netflix.metacat.connector.druid.converter.DruidConverterUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


/**
 * Druid implementation of the ConnectorPartitionService.
 * The partition concept is used to model segment in druid.
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class DruidConnectorPartitionService implements ConnectorPartitionService {
    private final MetacatDruidClient druidClient;
    private final DruidConnectorInfoConverter druidConnectorInfoConverter;

    /**
     * Constructor.
     *
     * @param druidClient                 druid client
     * @param druidConnectorInfoConverter druid infor converter
     */
    public DruidConnectorPartitionService(
        final MetacatDruidClient druidClient,
        final DruidConnectorInfoConverter druidConnectorInfoConverter
    ) {
        this.druidClient = druidClient;
        this.druidConnectorInfoConverter = druidConnectorInfoConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPartitionCount(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final TableInfo tableInfo
    ) {
        final ObjectNode node = this.druidClient.getAllDataByName(name.getTableName());
        return DruidConverterUtil.getSegmentCount(node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo) {
        final ObjectNode node = this.druidClient.getAllDataByName(name.getTableName());
        final DataSource dataSource = DruidConverterUtil.getDatasourceFromAllSegmentJsonObject(node);
        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        dataSource.getSegmentList().forEach(
            p -> partitionInfos.add(this.druidConnectorInfoConverter.getPartitionInfoFromSegment(p)));
        return partitionInfos;
    }
}
