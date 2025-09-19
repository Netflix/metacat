package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.connector.polaris.store.Utils;
import com.netflix.metacat.connector.polaris.store.base.BasePolarisTableReplicaRepository;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation for Custom repository using JdbcTemplate for storing PolarisTableEntity.
 */
@Repository
public class PolarisTableReplicaJDBC extends BasePolarisTableReplicaRepository {
    private final JdbcTemplate jdbcTemplate;

    /**
     * Configure to use the readerJdbcTemplate.
     * @param jdbcTemplate readerJdbcTemplate
     */
    @Autowired
    public PolarisTableReplicaJDBC(@Qualifier("readerJdbcTemplate") final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Finds all tables by database name and table prefix for the current page.
     *
     * @param dbName the database name
     * @param tableNamePrefix the table name prefix
     * @param page the pageable object containing pagination information
     * @param selectAllColumns flag indicating whether to select all columns or just the table name
     * @return a Slice of results
     */
    @Override
    protected <T> Slice<T> findAllTablesByDbNameAndTablePrefixForCurrentPage(
            final String catalogName,
        final String dbName,
        final String tableNamePrefix,
        final Pageable page,
        final boolean selectAllColumns) {

        final String orderBy = Utils.generateOrderBy(page);

        final String selectClause = selectAllColumns ? "t.*" : "t.tbl_name";
        final String sql = "SELECT " + selectClause + " FROM TBLS t "
            + "WHERE t.catalog_name = ? AND t.db_name = ? AND t.tbl_name LIKE ?" + orderBy
            + " LIMIT ? OFFSET ?";

        final List<T> resultList = jdbcTemplate.query(sql, new Object[]{
                catalogName,
            dbName,
            tableNamePrefix + "%",
            page.getPageSize() + 1,
            page.getPageNumber() * page.getPageSize(),
        }, new RowMapper<T>() {
            @Override
            public T mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                if (selectAllColumns) {
                    // Map the result set to PolarisTableEntity
                    final PolarisTableEntity entity = new PolarisTableEntity();
                    entity.setCatalogName(rs.getString("catalog_name"));
                    entity.setTblId(rs.getString("id"));
                    entity.setDbName(rs.getString("db_name"));
                    entity.setTblName(rs.getString("tbl_name"));
                    entity.setPreviousMetadataLocation(rs.getString("previous_metadata_location"));
                    entity.setMetadataLocation(rs.getString("metadata_location"));
                    // Map the AuditEntity
                    final AuditEntity audit = new AuditEntity();
                    audit.setCreatedBy(rs.getString("created_by"));
                    audit.setLastModifiedBy(rs.getString("last_updated_by"));
                    audit.setCreatedDate(rs.getTimestamp("created_date").toInstant());
                    audit.setLastModifiedDate(rs.getTimestamp("last_updated_date").toInstant());
                    entity.setAudit(audit);
                    return (T) entity;
                } else {
                    // Return only the table name
                    return (T) rs.getString("tbl_name");
                }
            }
        });

        // Check if there is a next page
        final boolean hasNext = resultList.size() > page.getPageSize();

        // If there is a next page, remove the last item from the list
        if (hasNext) {
            resultList.remove(resultList.size() - 1);
        }
        return new SliceImpl<>(resultList, page, hasNext);
    }
}
