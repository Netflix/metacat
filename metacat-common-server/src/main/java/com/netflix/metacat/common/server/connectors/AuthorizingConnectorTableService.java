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
import com.netflix.metacat.common.server.connectors.exception.TableLocationUpdateLockException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.util.ConnectorAuthorizationUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Connector decorator that authorizes calls based on SSO caller context
 * and/or locks table location updates.
 *
 * <p>Two independent features:</p>
 * <ul>
 *   <li><b>Authorization</b> — gates all operations by caller identity</li>
 *   <li><b>Location update lock</b> — blocks any update containing {@code metadata_location}</li>
 * </ul>
 *
 * @author jursetta
 */
public class AuthorizingConnectorTableService implements ConnectorTableService {

    private static final String METADATA_LOCATION_KEY = "metadata_location";

    @Getter
    @NonNull
    private final ConnectorTableService delegate;

    @NonNull
    private final Set<String> allowedCallers;

    @NonNull
    private final String catalogName;

    private final boolean authorizationEnabled;

    private final boolean locationUpdateLocked;

    /**
     * Creates a new authorizing/locking table service decorator.
     *
     * @param delegate              the underlying table service
     * @param allowedCallers        set of caller names allowed when authorization is enabled
     * @param catalogName           the catalog name for error messages
     * @param authorizationEnabled  whether caller-based authorization is enabled
     * @param locationUpdateLocked  whether metadata_location updates are blocked
     */
    public AuthorizingConnectorTableService(
        @NonNull final ConnectorTableService delegate,
        @NonNull final Set<String> allowedCallers,
        @NonNull final String catalogName,
        final boolean authorizationEnabled,
        final boolean locationUpdateLocked
    ) {
        this.delegate = delegate;
        this.allowedCallers = allowedCallers;
        this.catalogName = catalogName;
        this.authorizationEnabled = authorizationEnabled;
        this.locationUpdateLocked = locationUpdateLocked;
    }

    @Override
    public void create(final ConnectorRequestContext context, final TableInfo resource) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "create", resource.getName());
        }
        delegate.create(context, resource);
    }

    @Override
    public void update(final ConnectorRequestContext context, final TableInfo resource) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "update", resource.getName());
        }
        if (locationUpdateLocked
            && resource.getMetadata() != null
            && resource.getMetadata().containsKey(METADATA_LOCATION_KEY)) {
            throw new TableLocationUpdateLockException(catalogName, resource.getName());
        }
        delegate.update(context, resource);
    }

    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "delete", name);
        }
        delegate.delete(context, name);
    }

    @Override
    public TableInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "get", name);
        }
        return delegate.get(context, name);
    }

    @Override
    @SuppressFBWarnings
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "exists", name);
        }
        return delegate.exists(context, name);
    }

    @Override
    public List<TableInfo> list(
        final ConnectorRequestContext context,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "list", name);
        }
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
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "listNames", name);
        }
        return delegate.listNames(context, name, prefix, sort, pageable);
    }

    @Override
    public void rename(
        final ConnectorRequestContext context,
        final QualifiedName oldName,
        final QualifiedName newName
    ) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "rename", oldName);
        }
        delegate.rename(context, oldName, newName);
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(
        final ConnectorRequestContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        if (authorizationEnabled) {
            // No specific resource name available, check catalog-level access
            ConnectorAuthorizationUtil.checkAuthorization(
                catalogName, allowedCallers, "getTableNames", QualifiedName.ofCatalog(catalogName));
        }
        return delegate.getTableNames(context, uris, prefixSearch);
    }

    @Override
    public List<QualifiedName> getTableNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final String filter,
        final Integer limit
    ) {
        if (authorizationEnabled) {
            ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, "getTableNames", name);
        }
        return delegate.getTableNames(context, name, filter, limit);
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
