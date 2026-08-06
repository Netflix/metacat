package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseCustomReplicaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only repository listing databases from the Polaris replica.
 */
@Repository
public class PolarisDatabaseReplicaJDBC implements PolarisDatabaseCustomReplicaRepository {

    private static final RowMapper<Object> NAME_MAPPER = (rs, rowNum) -> rs.getString("name");

    private static final RowMapper<Object> ENTITY_MAPPER = (rs, rowNum) -> {
        final AuditEntity audit = new AuditEntity(
            rs.getString("created_by"),
            rs.getString("last_updated_by"),
            rs.getTimestamp("created_date").toInstant(),
            rs.getTimestamp("last_updated_date").toInstant()
        );
        return PolarisDatabaseEntity.builder()
            .dbId(rs.getString("id"))
            .catalogName(rs.getString("catalog_name"))
            .dbName(rs.getString("name"))
            .location(rs.getString("location"))
            .audit(audit)
            .build();
    };

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

    @Override
    public List<?> getAllDatabases(
        final String catalogName,
        final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final boolean selectAllColumns) {

        // Pages are walked by cursor, which requires ordering by name, the only column unique
        // within a catalog. A sort on any other column is ignored rather than skipping rows.
        final boolean descending = sort != null && "name".equals(sort.getSortBy())
            && sort.getOrder() == SortOrder.DESC;

        final String prefix = (dbNamePrefix == null ? "" : dbNamePrefix) + "%";
        final RowMapper<Object> mapper = selectAllColumns ? ENTITY_MAPPER : NAME_MAPPER;
        final String columns = selectAllColumns ? "d.*" : "d.name";

        final List<Object> retval = new ArrayList<>();
        String lastDbName = null;
        while (true) {
            // Seek past the last row of the previous page rather than offsetting into the result,
            // so every page is a range scan on the (catalog_name, name) index.
            final String seek = descending ? " AND d.name < ?" : " AND d.name > ?";
            final String sql = "SELECT " + columns + " FROM DBS d "
                + "WHERE d.catalog_name = ? AND d.name LIKE ?"
                + (lastDbName == null ? "" : seek)
                + " ORDER BY d.name " + (descending ? "DESC" : "ASC") + " LIMIT ?";
            final Object[] params = lastDbName == null
                ? new Object[]{catalogName, prefix, pageSize}
                : new Object[]{catalogName, prefix, lastDbName, pageSize};

            final List<Object> page = jdbcTemplate.query(sql, params, mapper);
            retval.addAll(page);
            if (page.size() < pageSize) {
                return retval;
            }
            final Object last = page.get(page.size() - 1);
            lastDbName = last instanceof PolarisDatabaseEntity
                ? ((PolarisDatabaseEntity) last).getDbName() : (String) last;
        }
    }
}
