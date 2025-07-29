package com.netflix.metacat.connector.polaris.store.jdbc;

import com.netflix.metacat.connector.polaris.store.Utils;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.base.BasePolarisDatabaseReplicaRepository;
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
 * Implementation for Custom repository using JdbcTemplate for interacting with databases.
 */
@Repository
public class PolarisDatabaseReplicaJDBC extends BasePolarisDatabaseReplicaRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Constructor to configure the JdbcTemplate.
     *
     * @param jdbcTemplate the JdbcTemplate to be used for database interactions
     */
    @Autowired
    public PolarisDatabaseReplicaJDBC(
        @Qualifier("readerJdbcTemplate") final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Retrieves a slice of databases filtered by a database name prefix for the current page.
     *
     * @param dbNamePrefix the prefix of the database name to filter results
     * @param page the pageable object containing pagination information
     * @param selectAllColumns flag indicating whether to select all columns or just the database name
     * @param <T> the type of the elements in the slice
     * @return a Slice of database results
     */
    @Override
    protected <T> Slice<T> getAllDatabasesForCurrentPage(
        final String dbNamePrefix, final Pageable page, final boolean selectAllColumns) {

        final String orderBy = Utils.generateOrderBy(page);

        // Generate Select clause
        final String selectClause = selectAllColumns ? "d.*" : "d.name";
        final String sql = "SELECT " + selectClause + " FROM DBS d "
            + "WHERE d.name LIKE ? " + orderBy
            + " LIMIT ? OFFSET ?";

        final List<T> resultList = jdbcTemplate.query(sql,
            new Object[]{dbNamePrefix + "%", page.getPageSize() + 1, page.getPageNumber() * page.getPageSize()},
            new RowMapper<T>() {
                @Override
                public T mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                    if (selectAllColumns) {
                        final AuditEntity audit = new AuditEntity(
                            rs.getString("created_by"),
                            rs.getString("last_updated_by"),
                            rs.getTimestamp("created_date").toInstant(),
                            rs.getTimestamp("last_updated_date").toInstant()
                        );
                        return (T) PolarisDatabaseEntity.builder()
                            .dbId(rs.getString("id"))
                            .dbName(rs.getString("name"))
                            .location(rs.getString("location"))
                            .audit(audit)
                            .build();
                    } else {
                        return (T) rs.getString("name");
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
