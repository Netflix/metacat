/*
 *  Copyright 2019 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.metacat.connector.hive.commonview;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.spectator.api.Registry;

/**
 * CommonViewHandler class.
 *
 * @author zhenl
 */
//TODO: in case a third iceberg table like object we should refactor them as a common iceberg-like handler
public class CommonViewHandler {
    protected final ConnectorContext connectorContext;
    protected final Registry registry;

    /**
     * CommonViewHandler Config
     * Constructor.
     *
     * @param connectorContext connector context
     */
    public CommonViewHandler(final ConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
        this.registry = connectorContext.getRegistry();
    }

    /**
     * get CommonView Table info.
     *
     * @param name              Common view name
     * @param tableLoc          table location
     * @param tableInfo         table info
     * @param hiveTypeConverter hive type converter
     * @return table info
     */
    public TableInfo getCommonViewTableInfo(final QualifiedName name,
                                            final String tableLoc,
                                            final TableInfo tableInfo,
                                            final HiveTypeConverter hiveTypeConverter) {
        return TableInfo.builder().build();
    }

    /**
     * Update common view column comments if the provided tableInfo has updated field comments.
     *
     * @param tableInfo table information
     * @return true if an update is done
     */
    public boolean update(final TableInfo tableInfo) {
        return false;
    }
}
