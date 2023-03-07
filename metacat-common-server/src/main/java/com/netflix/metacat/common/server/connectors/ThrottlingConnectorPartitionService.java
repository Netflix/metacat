package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext;
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

@Slf4j
@RequiredArgsConstructor
public class ThrottlingConnectorPartitionService implements ConnectorPartitionService {
    @Getter
    @NonNull
    private final ConnectorPartitionService delegate;
    @NonNull
    private final RateLimiter rateLimiter;

    @Override
    public List<PartitionInfo> getPartitions(final ConnectorRequestContext context,
                                             final QualifiedName table,
                                             final PartitionListRequest partitionsRequest,
                                             final TableInfo tableInfo) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), table);
        return delegate.getPartitions(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public PartitionsSaveResponse savePartitions(final ConnectorRequestContext context,
                                                 final QualifiedName table,
                                                 final PartitionsSaveRequest partitionsSaveRequest) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), table);
        return delegate.savePartitions(context, table, partitionsSaveRequest);
    }

    @Override
    public void deletePartitions(final ConnectorRequestContext context,
                                 final QualifiedName tableName,
                                 final List<String> partitionNames,
                                 final TableInfo tableInfo) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), tableName);
        delegate.deletePartitions(context, tableName, partitionNames, tableInfo);
    }

    @Override
    public int getPartitionCount(final ConnectorRequestContext context,
                                 final QualifiedName table,
                                 final TableInfo tableInfo) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), table);
        return delegate.getPartitionCount(context, table, tableInfo);
    }

    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(final ConnectorRequestContext context,
                                                              final List<String> uris,
                                                              final boolean prefixSearch) {
        return delegate.getPartitionNames(context, uris, prefixSearch);
    }

    @Override
    public List<String> getPartitionKeys(final ConnectorRequestContext context,
                                         final QualifiedName table,
                                         final PartitionListRequest partitionsRequest,
                                         final TableInfo tableInfo) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), table);
        return delegate.getPartitionKeys(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public List<String> getPartitionUris(final ConnectorRequestContext context,
                                         final QualifiedName table,
                                         final PartitionListRequest partitionsRequest,
                                         final TableInfo tableInfo) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), table);
        return delegate.getPartitionUris(context, table, partitionsRequest, tableInfo);
    }

    @Override
    public void create(final ConnectorRequestContext context, final PartitionInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final PartitionInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        delegate.delete(context, name);
    }

    @Override
    public PartitionInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.exists(context, name);
    }

    @Override
    public List<PartitionInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                    @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                    @Nullable final Pageable pageable) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(final ConnectorRequestContext context, final QualifiedName name,
                                         @Nullable final QualifiedName prefix,
                                         @Nullable final Sort sort, @Nullable final Pageable pageable) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context, final QualifiedName oldName,
                       final QualifiedName newName) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), oldName);
        delegate.rename(context, oldName, newName);
    }

    private void checkThrottling(final String requestName, final QualifiedName resource) {
        if (rateLimiter.hasExceededRequestLimit(new RateLimiterRequestContext(requestName, resource))) {
            final String errorMsg = String.format("Too many requests for resource %s. Request: %s",
                resource, requestName);
            log.warn(errorMsg);
            throw new MetacatTooManyRequestsException(errorMsg);
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

