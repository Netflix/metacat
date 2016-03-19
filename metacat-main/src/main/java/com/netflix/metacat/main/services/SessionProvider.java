package com.netflix.metacat.main.services;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.google.inject.Inject;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.connector.MetacatConnectorManager;

import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class SessionProvider {
    @Inject
    MetacatConnectorManager metacatConnectorManager;
    @Inject
    SessionPropertyManager sessionPropertyManager;

    public Session getSession(QualifiedName name) {
        if (name.isDatabaseDefinition() || name.isTableDefinition() || name.isPartitionDefinition() || name.isViewDefinition()) {
            return getSession(name.getCatalogName(), name.getDatabaseName());
        } else {
            return getSession(name.getCatalogName());
        }
    }

    private Session getSession(String catalogName) {
        return getSession(catalogName, "%");
    }

    private Session getSession(String catalogName, String databaseName) {
        String source = metacatConnectorManager.getCatalogConfig(catalogName).getType();
        MetacatContext context = MetacatContextManager.getContext();
        return Session.builder(sessionPropertyManager)
                .setIdentity(new Identity(context.getUserName(), Optional.empty()))
                .setRemoteUserAddress(context.getClientId())
                .setSource(source)
                .setCatalog(catalogName)
                .setSchema(databaseName)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }
}
