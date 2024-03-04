package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException;
import com.netflix.metacat.common.server.api.authorization.Authorization;
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Connector that authorizes requests based on the request context.
 */
@Slf4j
@RequiredArgsConstructor
public class AuthEnabledConnectorPartitionService implements ConnectorPartitionService {
    @Getter
    @NonNull
    private final ConnectorPartitionService delegate;
    @NonNull
    private final Authorization authorization;

    @Override
    public List<PartitionInfo> getPartitions(final ConnectorRequestContext context,
                                             final QualifiedName table,
                                             final PartitionListRequest partitionsRequest,
                                             final TableInfo tableInfo) {
        authorize(MetacatContextManager.getContext(), table.toString());
        return delegate.getPartitions(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public PartitionsSaveResponse savePartitions(final ConnectorRequestContext context,
                                                 final QualifiedName table,
                                                 final PartitionsSaveRequest partitionsSaveRequest) {
        authorize(MetacatContextManager.getContext(), table.toString());
        return delegate.savePartitions(context, table, partitionsSaveRequest);
    }

    @Override
    public void deletePartitions(final ConnectorRequestContext context,
                                 final QualifiedName tableName,
                                 final List<String> partitionNames,
                                 final TableInfo tableInfo) {
        authorize(MetacatContextManager.getContext(), tableName.toString());
        delegate.deletePartitions(context, tableName, partitionNames, tableInfo);
    }

    @Override
    public int getPartitionCount(final ConnectorRequestContext context,
                                 final QualifiedName table,
                                 final TableInfo tableInfo) {
        authorize(MetacatContextManager.getContext(), table.toString());
        return delegate.getPartitionCount(context, table, tableInfo);
    }

    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(final ConnectorRequestContext context,
                                                              final List<String> uris,
                                                              final boolean prefixSearch) {
        authorize(MetacatContextManager.getContext(), "N/A");
        return delegate.getPartitionNames(context, uris, prefixSearch);
    }

    @Override
    public List<String> getPartitionKeys(final ConnectorRequestContext context,
                                         final QualifiedName table,
                                         final PartitionListRequest partitionsRequest,
                                         final TableInfo tableInfo) {
        authorize(MetacatContextManager.getContext(), table.toString());
        return delegate.getPartitionKeys(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public List<String> getPartitionUris(final ConnectorRequestContext context,
                                         final QualifiedName table,
                                         final PartitionListRequest partitionsRequest,
                                         final TableInfo tableInfo) {
        authorize(MetacatContextManager.getContext(), table.toString());
        return delegate.getPartitionUris(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public void create(final ConnectorRequestContext context, final PartitionInfo resource) {
        authorize(MetacatContextManager.getContext(), resource.getName().toString());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final PartitionInfo resource) {
        authorize(MetacatContextManager.getContext(), resource.getName().toString());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name.toString());
        delegate.delete(context, name);
    }

    @Override
    public PartitionInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name.toString());
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name.toString());
        return delegate.exists(context, name);
    }

    @Override
    public List<PartitionInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                    @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                    @Nullable final Pageable pageable) {
        authorize(MetacatContextManager.getContext(), name.toString());
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(final ConnectorRequestContext context, final QualifiedName name,
                                         @Nullable final QualifiedName prefix,
                                         @Nullable final Sort sort, @Nullable final Pageable pageable) {
        authorize(MetacatContextManager.getContext(), name.toString());
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context, final QualifiedName oldName,
                       final QualifiedName newName) {
        authorize(MetacatContextManager.getContext(), oldName.toString());
        delegate.rename(context, oldName, newName);
    }

    private void authorize(final MetacatRequestContext context, final String resource) {
        final AuthorizationStatus status = authorization.isAuthorized(context);
        if (!status.isAuthorized()) {
            final String errorMsg = String.format("Forbidden request %s for resource %s. Details: %s",
                MetacatContextManager.getContext().getRequestName(), resource, status.getDetails());
            log.warn(errorMsg);
            throw new MetacatUnAuthorizedException(errorMsg);
        }
    }

    @Override
    public boolean equals(final Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}

