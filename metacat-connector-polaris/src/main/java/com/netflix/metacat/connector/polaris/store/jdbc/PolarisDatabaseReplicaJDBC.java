package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
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
     * @param sort         sort, honored only for the name column
     * @param pageSize     rows fetched per round trip
     * @return the matching database entities, ordered by name
     */
    public List<PolarisDatabaseEntity> getDatabases(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize) {
        return list(catalogName, dbNamePrefix, sort, pageSize, "d.*", ENTITY_MAPPER,
            PolarisDatabaseEntity::getDbName);
    }

    /**
     * Fetch all database names in a catalog matching the given name prefix.
     *
     * @param catalogName  catalog name
     * @param dbNamePrefix database name prefix, may be null or empty to match all
     * @param sort         sort, honored only for the name column
     * @param pageSize     rows fetched per round trip
     * @return the matching database names, ordered by name
     */
    public List<String> getDatabaseNames(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize) {
        return list(catalogName, dbNamePrefix, sort, pageSize, "d.name", NAME_MAPPER, Function.identity());
    }

    private <T> List<T> list(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final String columns,
        final RowMapper<T> mapper,
        final Function<T, String> cursorOf) {

        // Pages are walked by cursor, which requires ordering by name, the only column unique
        // within a catalog. A sort on any other column is ignored rather than skipping rows.
        final boolean desc = sort != null && "name".equals(sort.getSortBy()) && sort.getOrder() == SortOrder.DESC;
        final String prefix = (dbNamePrefix == null ? "" : dbNamePrefix) + "%";

        final String head = "SELECT " + columns + " FROM DBS d WHERE d.catalog_name = ? AND d.name LIKE ?";
        final String seek = desc ? " AND d.name < ?" : " AND d.name > ?";
        final String tail = " ORDER BY d.name " + (desc ? "DESC" : "ASC") + " LIMIT ?";

        final List<T> retval = new ArrayList<>();
        String cursor = null;
        while (true) {
            final List<T> page = cursor == null
                ? jdbcTemplate.query(head + tail, mapper, catalogName, prefix, pageSize)
                : jdbcTemplate.query(head + seek + tail, mapper, catalogName, prefix, cursor, pageSize);
            retval.addAll(page);
            if (page.size() < pageSize) {
                return retval;
            }
            cursor = cursorOf.apply(page.get(page.size() - 1));
        }
    }
}
