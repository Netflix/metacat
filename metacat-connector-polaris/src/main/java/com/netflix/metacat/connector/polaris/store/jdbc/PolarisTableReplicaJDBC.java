package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
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
 * Read-only repository listing tables from the Polaris replica.
 */
@Repository
public class PolarisTableReplicaJDBC {

    private static final RowMapper<String> NAME_MAPPER = (rs, rowNum) -> rs.getString("tbl_name");

    private static final RowMapper<PolarisTableEntity> ENTITY_MAPPER = (rs, rowNum) ->
        PolarisTableEntity.builder()
            .tblId(rs.getString("id"))
            .catalogName(rs.getString("catalog_name"))
            .dbName(rs.getString("db_name"))
            .tblName(rs.getString("tbl_name"))
            .previousMetadataLocation(rs.getString("previous_metadata_location"))
            .metadataLocation(rs.getString("metadata_location"))
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
    public PolarisTableReplicaJDBC(@Qualifier("readerJdbcTemplate") final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Fetch all table entities in a database matching the given name prefix.
     *
     * @param catalogName     catalog name
     * @param dbName          database name
     * @param tableNamePrefix table name prefix, may be null or empty to match all
     * @param pageSize        rows fetched per round trip
     * @return the matching table entities, ordered by table name
     */
    public List<PolarisTableEntity> getTableEntities(
        final String catalogName,
        final String dbName,
        @Nullable final String tableNamePrefix,
        final int pageSize) {
        return list(catalogName, dbName, tableNamePrefix, pageSize, "t.*", ENTITY_MAPPER,
            PolarisTableEntity::getTblName);
    }

    /**
     * Fetch all table names in a database matching the given name prefix.
     *
     * @param catalogName     catalog name
     * @param dbName          database name
     * @param tableNamePrefix table name prefix, may be null or empty to match all
     * @param pageSize        rows fetched per round trip
     * @return the matching table names, ordered by table name
     */
    public List<String> getTableNames(
        final String catalogName,
        final String dbName,
        @Nullable final String tableNamePrefix,
        final int pageSize) {
        return list(catalogName, dbName, tableNamePrefix, pageSize, "t.tbl_name", NAME_MAPPER, Function.identity());
    }

    private <T> List<T> list(
        final String catalogName,
        final String dbName,
        @Nullable final String tableNamePrefix,
        final int pageSize,
        final String columns,
        final RowMapper<T> mapper,
        final Function<T, String> cursorOf) {

        final String prefix = (tableNamePrefix == null ? "" : tableNamePrefix) + "%";

        // Pages are walked by cursor rather than offset, so every page is a range scan on the
        // (catalog_name, db_name, tbl_name) index instead of a rescan from the start.
        final String head = "SELECT " + columns + " FROM TBLS t "
            + "WHERE t.catalog_name = ? AND t.db_name = ? AND t.tbl_name LIKE ?";
        final String tail = " ORDER BY t.tbl_name ASC LIMIT ?";

        final List<T> retval = new ArrayList<>();
        String cursor = null;
        while (true) {
            final List<T> page = cursor == null
                ? jdbcTemplate.query(head + tail, mapper, catalogName, dbName, prefix, pageSize)
                : jdbcTemplate.query(head + " AND t.tbl_name > ?" + tail, mapper,
                    catalogName, dbName, prefix, cursor, pageSize);
            retval.addAll(page);
            if (page.size() < pageSize) {
                return retval;
            }
            cursor = cursorOf.apply(page.get(page.size() - 1));
        }
    }
}
