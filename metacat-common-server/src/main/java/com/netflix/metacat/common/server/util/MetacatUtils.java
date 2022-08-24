//CHECKSTYLE:OFF
package com.netflix.metacat.common.server.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.spectator.api.Registry;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * General metacat utility methods.
 */
public class MetacatUtils {

    /**
     * Default Ctor.
     */
    private MetacatUtils() {
    }

    /**
     * List files in a given dir.
     *
     * @param dir The directory.
     * @return List of files if any.
     */
    public static List<File> listFiles(final File dir) {
        if (dir != null && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    /**
     * Load properties from a file.
     *
     * @param file Properties file.
     * @return A Map of properties.
     * @throws Exception
     */
    public static Map<String, String> loadProperties(final File file) throws Exception {
        Preconditions.checkNotNull(file, "file is null");

        final Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return Maps.fromProperties(properties);
    }

    public static ConnectorContext buildConnectorContext(
            final File file,
            final String connectorType,
            final Config config,
            final Registry registry,
            final ApplicationContext applicationContext,
            final Map<String, String> properties) {
        // Catalog shard name should be unique. Usually the catalog name is same as the catalog shard name.
        // If multiple catalog property files refer the same catalog name, then there will be multiple shard names
        // with the same catalog name.
        final String catalogShardName = Files.getNameWithoutExtension(file.getName());

        // If catalog name is not specified, then use the catalog shard name.
        final String catalogName = properties.getOrDefault(MetacatCatalogConfig.Keys.CATALOG_NAME, catalogShardName);

        return new ConnectorContext(catalogName, catalogShardName, connectorType, config, registry,
                applicationContext, properties);
    }
}
