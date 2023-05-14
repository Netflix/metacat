//CHECKSTYLE:OFF
package com.netflix.metacat.common.server.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.spectator.api.Registry;
import org.springframework.context.ApplicationContext;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * General metacat utility methods.
 */
public class MetacatUtils {

    public static final String ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG = "iceberg_migration_do_not_modify";
    public static final String NAME_TAGS = "tags";

    /**
     * Iceberg common view field names.
     */
    public static final String COMMON_VIEW = "common_view";
    public static final String STORAGE_TABLE = "storage_table";


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
     * @throws Exception IOException on failure to load file.
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

    public static boolean configHasDoNotModifyForIcebergMigrationTag(final Set<String> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptySet()).stream().
                anyMatch(t -> t.trim().equalsIgnoreCase(ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG));
    }

    public static boolean hasDoNotModifyForIcebergMigrationTag(@Nullable final TableDto tableDto,
                                                               final Set<String> tags) {
        if (tableDto != null) {
            final Set<String> tableTags = getTableTags(tableDto.getDefinitionMetadata());
            if (tableTags != null) {
                return configHasDoNotModifyForIcebergMigrationTag(tags) &&
                        tableTags.stream().anyMatch(t -> t.trim().equalsIgnoreCase(ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG));
            }
        }
        return false;
    }

    public static Set<String> getTableTags(@Nullable final ObjectNode definitionMetadata) {
        final Set<String> tags = Sets.newHashSet();
        if (definitionMetadata != null && definitionMetadata.get(NAME_TAGS) != null) {
            final JsonNode tagsNode = definitionMetadata.get(NAME_TAGS);
            if (tagsNode.isArray() && tagsNode.size() > 0) {
                for (JsonNode tagNode : tagsNode) {
                    tags.add(tagNode.textValue().trim());
                }
            }
        }
        return tags;
    }

    public static String getIcebergMigrationExceptionMsg(final String requestType,
                                                         final String tableName) {
        return String.format("%s to hive table: %s are temporarily blocked " +
                        "for automated migration to iceberg. Please retry",
                requestType, tableName);
    }

    /**
     * check if the table is a common view.
     *
     * @param tableMetadata table metadata map
     * @return true for common view
     */
    public static boolean isCommonView(final Map<String, String> tableMetadata) {
        return tableMetadata != null && Boolean.parseBoolean(tableMetadata.get(COMMON_VIEW));
    }

    /**
     * Returns the name of the common view storage table if present.
     *
     * @param tableMetadata the table metadata
     * @return Storage table name.
     */
    public static Optional<String> getCommonViewStorageTable(final Map<String, String> tableMetadata) {
        if (tableMetadata != null) {
            return Optional.ofNullable(tableMetadata.get(STORAGE_TABLE));
        }
        return Optional.empty();
    }
}
