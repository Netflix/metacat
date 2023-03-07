package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext;
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
 * Connector that throttles calls to the connector based on the contextual request name
 * and the resource. Not all APIs can be throttles since we may not have a resource
 * but those are a small monitory
 */
@Slf4j
@RequiredArgsConstructor
public class ThrottlingConnectorTableService implements ConnectorTableService {
    @Getter
    @NonNull
    private final ConnectorTableService delegate;
    @NonNull
    private final RateLimiter rateLimiter;

    @Override
    public TableInfo get(final ConnectorRequestContext context,
                         final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.get(context, name);
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(final ConnectorRequestContext context,
                                                          final List<String> uris,
                                                          final boolean prefixSearch) {
        return delegate.getTableNames(context, uris, prefixSearch);
    }

    @Override
    public List<QualifiedName> getTableNames(final ConnectorRequestContext context,
                                             final QualifiedName name,
                                             final String filter,
                                             final Integer limit) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.getTableNames(context, name, filter, limit);
    }

    @Override
    public void create(final ConnectorRequestContext context, final TableInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final TableInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        delegate.delete(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.exists(context, name);
    }

    @Override
    public List<TableInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                @Nullable final QualifiedName prefix,
                                @Nullable final Sort sort, @Nullable final Pageable pageable) {
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
    public void rename(final ConnectorRequestContext context,
                       final QualifiedName oldName,
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
