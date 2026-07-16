/*
 *
 *  Copyright 2024 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Connector decorator that authorizes partition service calls based on SSO caller context.
 * Only callers in the allowed list can access this connector's operations.
 *
 * @author jursetta
 */
@RequiredArgsConstructor
public class AuthorizingConnectorPartitionService implements ConnectorPartitionService {

    @Getter
    @NonNull
    private final ConnectorPartitionService delegate;

    @NonNull
    private final ConnectorAuthorizer authorizer;

    @NonNull
    private final String authorizedCallers;

    @NonNull
    private final String catalogName;

    private void authorize(final String operation, final QualifiedName resource) {
        authorizer.checkAuthorization(catalogName, authorizedCallers, operation, resource);
    }

    @Override
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo
    ) {
        authorize("getPartitions", table);
        return delegate.getPartitions(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public PartitionsSaveResponse savePartitions(
        final ConnectorRequestContext context,
        final QualifiedName table,
        final PartitionsSaveRequest partitionsSaveRequest
    ) {
        authorize("savePartitions", table);
        return delegate.savePartitions(context, table, partitionsSaveRequest);
    }

    @Override
    public void deletePartitions(
        final ConnectorRequestContext context,
        final QualifiedName tableName,
        final List<String> partitionNames,
        final TableInfo tableInfo
    ) {
        authorize("deletePartitions", tableName);
        delegate.deletePartitions(context, tableName, partitionNames, tableInfo);
    }

    @Override
    public int getPartitionCount(
        final ConnectorRequestContext context,
        final QualifiedName table,
        final TableInfo tableInfo
    ) {
        authorize("getPartitionCount", table);
        return delegate.getPartitionCount(context, table, tableInfo);
    }

    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(
        final ConnectorRequestContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        // No specific resource name available, check catalog-level access
        authorize("getPartitionNames", QualifiedName.ofCatalog(catalogName));
        return delegate.getPartitionNames(context, uris, prefixSearch);
    }

    @Override
    public List<String> getPartitionKeys(
        final ConnectorRequestContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo
    ) {
        authorize("getPartitionKeys", table);
        return delegate.getPartitionKeys(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public List<String> getPartitionUris(
        final ConnectorRequestContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo
    ) {
        authorize("getPartitionUris", table);
        return delegate.getPartitionUris(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public void create(final ConnectorRequestContext context, final PartitionInfo resource) {
        authorize("create", resource.getName());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final PartitionInfo resource) {
        authorize("update", resource.getName());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        authorize("delete", name);
        delegate.delete(context, name);
    }

    @Override
    public PartitionInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        authorize("get", name);
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        authorize("exists", name);
        return delegate.exists(context, name);
    }

    @Override
    public List<PartitionInfo> list(
        final ConnectorRequestContext context,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        authorize("list", name);
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        authorize("listNames", name);
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(
        final ConnectorRequestContext context,
        final QualifiedName oldName,
        final QualifiedName newName
    ) {
        authorize("rename", oldName);
        delegate.rename(context, oldName, newName);
    }

    /**
     * Delegates equals to the underlying service so that decorated services
     * representing the same catalog are considered equal. This allows
     * ConnectorManager to deduplicate services in its sets.
     */
    @Override
    public boolean equals(final Object o) {
        return delegate.equals(o);
    }

    /**
     * Delegates hashCode to the underlying service for consistency with equals.
     */
    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
