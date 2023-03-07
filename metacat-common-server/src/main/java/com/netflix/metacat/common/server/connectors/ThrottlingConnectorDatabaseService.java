package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ThrottlingConnectorDatabaseService implements ConnectorDatabaseService {
    @Getter
    @NonNull
    private final ConnectorDatabaseService delegate;
    @NonNull
    private final RateLimiter rateLimiter;

    @Override
    public void create(final ConnectorRequestContext context, final DatabaseInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo resource) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        delegate.delete(context, name);
    }

    @Override
    public DatabaseInfo get(final ConnectorRequestContext context, final QualifiedName name) {
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
    public List<DatabaseInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                   @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                   @Nullable final Pageable pageable) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(final ConnectorRequestContext context, final QualifiedName name,
                                         @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                         @Nullable final Pageable pageable) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context, final QualifiedName oldName,
                       final QualifiedName newName) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), oldName);
        delegate.rename(context, oldName, newName);
    }

    @Override
    public List<QualifiedName> listViewNames(final ConnectorRequestContext context, final QualifiedName databaseName) {
        checkThrottling(MetacatContextManager.getContext().getRequestName(), databaseName);
        return delegate.listViewNames(context, databaseName);
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
