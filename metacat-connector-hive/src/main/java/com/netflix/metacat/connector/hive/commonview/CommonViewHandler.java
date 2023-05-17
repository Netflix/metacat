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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Throwables;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.spectator.api.Registry;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;

import java.util.concurrent.ExecutionException;

/**
 * CommonViewHandler class.
 *
 * @author zhenl
 */
//TODO: in case a third iceberg table like object we should refactor them as a common iceberg-like handler
@CacheConfig(cacheNames = "metacat")
public class CommonViewHandler {
    private static final Retryer<Void> RETRY_ICEBERG_TABLE_UPDATE = RetryerBuilder.<Void>newBuilder()
        .retryIfExceptionOfType(TablePreconditionFailedException.class)
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
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
        return TableInfo.builder().name(name).auditInfo(tableInfo.getAudit())
                .fields(tableInfo.getFields()).serde(tableInfo.getSerde())
                .metadata(tableInfo.getMetadata()).build();
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

    /**
     * Handle common view update request using iceberg table
     * update strategy for common views that employs iceberg library.
     *
     * @param requestContext    request context
     * @param directSqlTable    direct sql table object
     * @param tableInfo         table info
     * @param tableMetadataLocation the common view table metadata location.
     */
    @CacheEvict(key = "'iceberg.view.' + #tableMetadataLocation", beforeInvocation = true)
    public void handleUpdate(final ConnectorRequestContext requestContext,
                             final DirectSqlTable directSqlTable,
                             final TableInfo tableInfo,
                             final String tableMetadataLocation) {
        requestContext.setIgnoreErrorsAfterUpdate(true);
        final boolean viewUpdated = this.update(tableInfo);
        if (viewUpdated) {
            try {
                RETRY_ICEBERG_TABLE_UPDATE.call(() -> {
                    try {
                        directSqlTable.updateIcebergTable(tableInfo);
                    } catch (TablePreconditionFailedException e) {
                        tableInfo.getMetadata()
                                .put(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION, e.getMetadataLocation());
                        this.update(tableInfo);
                        throw e;
                    }
                    return null;
                });
            } catch (RetryException e) {
                Throwables.propagate(e.getLastFailedAttempt().getExceptionCause());
            } catch (ExecutionException e) {
                Throwables.propagate(e.getCause());
            }
        } else {
            directSqlTable.updateIcebergTable(tableInfo);
        }
    }
}
