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
import com.netflix.metacat.common.server.connectors.model.CatalogInfo;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Connector that validates requests based on the request context and/or resource.
 */
@Slf4j
@RequiredArgsConstructor
public class ValidatingConnectorCatalogService implements ConnectorCatalogService {
    @Getter
    @NonNull
    private final ConnectorCatalogService delegate;
    @NonNull
    private final RateLimiter rateLimiter;
    private final boolean rateLimiterEnabled;
    @NonNull
    private final Authorization authorization;
    private final boolean authorizationEnabled;

    @Override
    public void create(final ConnectorRequestContext context, final CatalogInfo resource) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), resource.getName());
        }
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final CatalogInfo resource) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), resource.getName());
        }
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name);
        }
        delegate.delete(context, name);
    }

    @Override
    public CatalogInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name);
        }
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name);
        }
        return delegate.exists(context, name);
    }

    @Override
    public List<CatalogInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                  @Nullable final QualifiedName prefix,
                                  @Nullable final Sort sort, @Nullable final Pageable pageable) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name);
        }
        return delegate.list(context, name, prefix, sort, pageable);
    }

    @Override
    public List<QualifiedName> listNames(final ConnectorRequestContext context, final QualifiedName name,
                                         @Nullable final QualifiedName prefix,
                                         @Nullable final Sort sort, @Nullable final Pageable pageable) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name);
        }
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context, final QualifiedName oldName,
                       final QualifiedName newName) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), oldName);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), oldName);
        }
        delegate.rename(context, oldName, newName);
    }

    /**
     * Throttles calls to the connector based on the contextual request name
     * and the resource.
     */
    private void checkThrottling(final String requestName, final QualifiedName resource) {
        if (rateLimiter.hasExceededRequestLimit(new RateLimiterRequestContext(requestName, resource))) {
            final String errorMsg = String.format("Too many requests for resource %s. Request: %s",
                resource, requestName);
            log.warn(errorMsg);
            throw new MetacatTooManyRequestsException(errorMsg);
        }
    }

    /**
     * Authorizes calls to the connector based on the request context.
     */
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