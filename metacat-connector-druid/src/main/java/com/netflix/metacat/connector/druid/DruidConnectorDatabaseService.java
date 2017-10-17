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

import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Druid Connector DatabaseService.
 * No database concept in druid.
 * @author zhenl
 * @since 1.2.0
 */
public class DruidConnectorDatabaseService implements ConnectorDatabaseService {

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        final ConnectorRequestContext requestContext,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable) {
        return Lists.newArrayList(QualifiedName.ofDatabase(name.getCatalogName(), DruidConfigConstants.DRUID_DB));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        return DatabaseInfo.builder()
            .name(QualifiedName.ofDatabase(name.getCatalogName(), DruidConfigConstants.DRUID_DB))
            .build();
    }
}
