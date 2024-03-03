package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException;
import com.netflix.metacat.common.server.api.authorization.Authorization;
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus;
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

/**
 * Connector that authorizes requests based on the request context.
 */
@Slf4j
@RequiredArgsConstructor
public class AuthEnabledConnectorDatabaseService implements ConnectorDatabaseService {
    @Getter
    @NonNull
    private final ConnectorDatabaseService delegate;
    @NonNull
    private final Authorization authorization;

    @Override
    public void create(final ConnectorRequestContext context, final DatabaseInfo resource) {
        authorize(MetacatContextManager.getContext(), resource.getName());
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo resource) {
        authorize(MetacatContextManager.getContext(), resource.getName());
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name);
        delegate.delete(context, name);
    }

    @Override
    public DatabaseInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name);
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        authorize(MetacatContextManager.getContext(), name);
        return delegate.exists(context, name);
    }

    @Override
    public List<DatabaseInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                   @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                   @Nullable final Pageable pageable) {
        authorize(MetacatContextManager.getContext(), name);
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(final ConnectorRequestContext context, final QualifiedName name,
                                         @Nullable final QualifiedName prefix, @Nullable final Sort sort,
                                         @Nullable final Pageable pageable) {
        authorize(MetacatContextManager.getContext(), name);
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context, final QualifiedName oldName,
                       final QualifiedName newName) {
        authorize(MetacatContextManager.getContext(), oldName);
        delegate.rename(context, oldName, newName);
    }

    @Override
    public List<QualifiedName> listViewNames(final ConnectorRequestContext context, final QualifiedName databaseName) {
        authorize(MetacatContextManager.getContext(), databaseName);
        return delegate.listViewNames(context, databaseName);
    }

    private void authorize(final MetacatRequestContext context, final QualifiedName resource) {
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
