package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Read-only repository listing databases from the Polaris replica.
 */
@Repository
public class PolarisDatabaseReplicaJDBC {

    private static final RowMapper<String> NAME_MAPPER = (rs, rowNum) -> rs.getString("name");

    private static final RowMapper<PolarisDatabaseEntity> ENTITY_MAPPER = (rs, rowNum) ->
        PolarisDatabaseEntity.builder()
            .dbId(rs.getString("id"))
            .catalogName(rs.getString("catalog_name"))
            .dbName(rs.getString("name"))
            .location(rs.getString("location"))
            .audit(AuditEntity.builder()
                .createdBy(rs.getString("created_by"))
                .lastModifiedBy(rs.getString("last_updated_by"))
                .createdDate(rs.getTimestamp("created_date").toInstant())
                .lastModifiedDate(rs.getTimestamp("last_updated_date").toInstant())
                .build())
            .build();

    private final JdbcTemplate jdbcTemplate;

    /**
     * Configure to use the readerJdbcTemplate.
     *
     * @param jdbcTemplate readerJdbcTemplate
     */
    @Autowired
    public PolarisDatabaseReplicaJDBC(@Qualifier("readerJdbcTemplate") final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Fetch all database entities in a catalog matching the given name prefix.
     *
     * @param catalogName  catalog name
     * @param dbNamePrefix database name prefix, may be null or empty to match all
     * @param pageSize     rows fetched per round trip
     * @return the matching database entities, ordered by name ascending
     */
    public List<PolarisDatabaseEntity> getDatabases(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        final int pageSize) {
        return list(catalogName, dbNamePrefix, pageSize, "d.*", ENTITY_MAPPER, PolarisDatabaseEntity::getDbName);
    }

    /**
     * Fetch all database names in a catalog matching the given name prefix.
     *
     * @param catalogName  catalog name
     * @param dbNamePrefix database name prefix, may be null or empty to match all
     * @param pageSize     rows fetched per round trip
     * @return the matching database names, ordered ascending
     */
    public List<String> getDatabaseNames(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        final int pageSize) {
        return list(catalogName, dbNamePrefix, pageSize, "d.name", NAME_MAPPER, Function.identity());
    }

    private <T> List<T> list(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        final int pageSize,
        final String columns,
        final RowMapper<T> mapper,
        final Function<T, String> cursorOf) {

        final String prefix = (dbNamePrefix == null ? "" : dbNamePrefix) + "%";

        // Pages are walked by cursor rather than offset, so every page is a range scan on the
        // (catalog_name, name) index instead of a rescan from the start.
        final String head = "SELECT " + columns + " FROM DBS d WHERE d.catalog_name = ? AND d.name LIKE ?";
        final String tail = " ORDER BY d.name ASC LIMIT ?";

        final List<T> retval = new ArrayList<>();
        String cursor = null;
        while (true) {
            final List<T> page = cursor == null
                ? jdbcTemplate.query(head + tail, mapper, catalogName, prefix, pageSize)
                : jdbcTemplate.query(head + " AND d.name > ?" + tail, mapper, catalogName, prefix, cursor, pageSize);
            retval.addAll(page);
            if (page.size() < pageSize) {
                return retval;
            }
            cursor = cursorOf.apply(page.get(page.size() - 1));
        }
    }
}
