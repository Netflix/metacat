package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableCustomReplicaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Read-only repository listing tables from the Polaris replica.
 */
@Repository
public class PolarisTableReplicaJDBC implements PolarisTableCustomReplicaRepository {

    private static final RowMapper<Object> NAME_MAPPER = (rs, rowNum) -> rs.getString("tbl_name");

    private static final RowMapper<Object> ENTITY_MAPPER = (rs, rowNum) -> {
        final PolarisTableEntity entity = new PolarisTableEntity();
        entity.setCatalogName(rs.getString("catalog_name"));
        entity.setTblId(rs.getString("id"));
        entity.setDbName(rs.getString("db_name"));
        entity.setTblName(rs.getString("tbl_name"));
        entity.setPreviousMetadataLocation(rs.getString("previous_metadata_location"));
        entity.setMetadataLocation(rs.getString("metadata_location"));
        final AuditEntity audit = new AuditEntity();
        audit.setCreatedBy(rs.getString("created_by"));
        audit.setLastModifiedBy(rs.getString("last_updated_by"));
        audit.setCreatedDate(rs.getTimestamp("created_date").toInstant());
        audit.setLastModifiedDate(rs.getTimestamp("last_updated_date").toInstant());
        entity.setAudit(audit);
        return entity;
    };

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

    @Override
    public List<?> findAllTablesByDbNameAndTablePrefix(
        final String catalogName,
        final String dbName,
        final String tableNamePrefix,
        final int pageSize,
        final boolean selectAllColumns) {

        final String prefix = (tableNamePrefix == null ? "" : tableNamePrefix) + "%";
        final RowMapper<Object> mapper = selectAllColumns ? ENTITY_MAPPER : NAME_MAPPER;
        final String columns = selectAllColumns ? "t.*" : "t.tbl_name";

        final List<Object> retval = new ArrayList<>();
        String lastTblName = null;
        while (true) {
            // Seek past the last row of the previous page rather than offsetting into the result,
            // so every page is a range scan on the (catalog_name, db_name, tbl_name) index.
            final String sql = "SELECT " + columns + " FROM TBLS t "
                + "WHERE t.catalog_name = ? AND t.db_name = ? AND t.tbl_name LIKE ?"
                + (lastTblName == null ? "" : " AND t.tbl_name > ?")
                + " ORDER BY t.tbl_name ASC LIMIT ?";
            final Object[] params = lastTblName == null
                ? new Object[]{catalogName, dbName, prefix, pageSize}
                : new Object[]{catalogName, dbName, prefix, lastTblName, pageSize};

            final List<Object> page = jdbcTemplate.query(sql, params, mapper);
            retval.addAll(page);
            if (page.size() < pageSize) {
                return retval;
            }
            final Object last = page.get(page.size() - 1);
            lastTblName = last instanceof PolarisTableEntity
                ? ((PolarisTableEntity) last).getTblName() : (String) last;
        }
    }
}
