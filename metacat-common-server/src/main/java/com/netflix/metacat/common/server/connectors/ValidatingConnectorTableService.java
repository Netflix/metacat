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
 * Connector that validates requests based on the request context and/or resource.
 */
@Slf4j
@RequiredArgsConstructor
public class ValidatingConnectorTableService implements ConnectorTableService {
    @Getter
    @NonNull
    private final ConnectorTableService delegate;
    @NonNull
    private final RateLimiter rateLimiter;
    private final boolean rateLimiterEnabled;
    @NonNull
    private final Authorization authorization;
    private final boolean authorizationEnabled;

    @Override
    public TableInfo get(final ConnectorRequestContext context,
                         final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name.toString());
        }
        return delegate.get(context, name);
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(final ConnectorRequestContext context,
                                                          final List<String> uris,
                                                          final boolean prefixSearch) {
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), "N/A");
        }
        return delegate.getTableNames(context, uris, prefixSearch);
    }

    @Override
    public List<QualifiedName> getTableNames(final ConnectorRequestContext context,
                                             final QualifiedName name,
                                             final String filter,
                                             final Integer limit) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name.toString());
        }
        return delegate.getTableNames(context, name, filter, limit);
    }

    @Override
    public void create(final ConnectorRequestContext context, final TableInfo resource) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), resource.toString());
        }
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final TableInfo resource) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), resource.getName());
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), resource.toString());
        }
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name.toString());
        }
        delegate.delete(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name.toString());
        }
        return delegate.exists(context, name);
    }

    @Override
    public List<TableInfo> list(final ConnectorRequestContext context, final QualifiedName name,
                                @Nullable final QualifiedName prefix,
                                @Nullable final Sort sort, @Nullable final Pageable pageable) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), name);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), name.toString());
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
            authorize(MetacatContextManager.getContext(), name.toString());
        }
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(final ConnectorRequestContext context,
                       final QualifiedName oldName,
                       final QualifiedName newName) {
        if (rateLimiterEnabled) {
            checkThrottling(MetacatContextManager.getContext().getRequestName(), oldName);
        }
        if (authorizationEnabled) {
            authorize(MetacatContextManager.getContext(), oldName.toString());
        }
        delegate.rename(context, oldName, newName);
    }

    /**
     * Throttles calls to the connector based on the contextual request name
     * and the resource. Only APIs with a resource are subject to throttling
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
     * Authorizes calls to the connector based on the request context
     */
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
