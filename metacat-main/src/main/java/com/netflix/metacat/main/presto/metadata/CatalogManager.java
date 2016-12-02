/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.metacat.main.presto.metadata;

import com.facebook.presto.metadata.CatalogManagerConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.metacat.main.presto.connector.ConnectorManager;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Catalog manager.
 */
@Slf4j
public class CatalogManager {
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();

    /**
     * Constructor.
     * @param connectorManager manager
     * @param config config
     */
    @Inject
    public CatalogManager(final ConnectorManager connectorManager, final CatalogManagerConfig config) {
        this(connectorManager, config.getCatalogConfigurationDir());
    }

    /**
     * Constructor.
     * @param connectorManager manager
     * @param catalogConfigurationDir config directory
     */
    public CatalogManager(final ConnectorManager connectorManager, final File catalogConfigurationDir) {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
    }

    /**
     * Returns true if all catalogs are loaded.
     * @return true if all catalogs are loaded
     */
    public boolean areCatalogsLoaded() {
        return catalogsLoaded.get();
    }

    /**
     * Loads catalogs.
     * @throws Exception error
     */
    public void loadCatalogs()
        throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        catalogsLoaded.set(true);
    }

    private void loadCatalog(final File file)
        throws Exception {
        log.info("-- Loading catalog %s --", file);
        final Map<String, String> properties = new HashMap<>(loadProperties(file));

        final String connectorName = properties.remove("connector.name");
        Preconditions.checkState(connectorName != null, "Catalog configuration %s does not contain conector.name",
            file.getAbsoluteFile());

        final String catalogName = Files.getNameWithoutExtension(file.getName());

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(final File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            final File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static Map<String, String> loadProperties(final File file)
        throws Exception {
        Preconditions.checkNotNull(file, "file is null");

        final Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return Maps.fromProperties(properties);
    }
}
