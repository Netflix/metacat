package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parent Child Relationship Metadata Service.
 * This stores the parent child relationship of two entities as first class citizen in Metacat.
 */
@Slf4j
@SuppressFBWarnings
@Transactional("metadataTxManager")
public class MySqlParentChildRelMetaDataService implements ParentChildRelMetadataService {
    // For now,
    // First where clause ? = parent : Prevent the table that is a child for another to be created as a parent
    // Second where clause ? = child : Prevent the child having more than one parent
    static final String SQL_CREATE_PARENT_CHILD_RELATIONS =
        "INSERT INTO parent_child_relation (parent, child, relation_type) "
            + "SELECT ?, ?, ? "
            + "WHERE NOT EXISTS (SELECT 1 FROM parent_child_relation WHERE child = ?) "
            + "AND NOT EXISTS (SELECT 1 FROM parent_child_relation WHERE child = ?)";

    static final String SQL_RENAME_ENTITY = "UPDATE parent_child_relation "
        + "SET parent = ?, child = ? WHERE parent = ? OR child = ?";

    static final String SQL_DROP_ENTITY = "DELETE FROM parent_child_relation "
        + "WHERE child = ? "
        + "AND (SELECT COUNT(*) FROM parent_child_relation WHERE parent = ?) = 0";

    static final String SQL_GET_PARENTS = "SELECT parent, relation_type FROM parent_child_relation WHERE child = ?";

    static final String SQL_GET_CHILDREN = "SELECT child, relation_type FROM parent_child_relation WHERE parent = ?";

    private JdbcTemplate jdbcTemplate;
    private RetryTemplate retryTemplate;

    /**
     * Constructor.
     *
     * @param jdbcTemplate jdbc template
     * @param retryTemplate retry template
     */
    @Autowired
    public MySqlParentChildRelMetaDataService(
        final JdbcTemplate jdbcTemplate,
        final RetryTemplate retryTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.retryTemplate = retryTemplate;
    }

    @Override
    public void createParentChildRelation(final QualifiedName parentName,
                                          final QualifiedName childName,
                                          final String type) {
        final int count = retryTemplate.execute((RetryCallback<Integer, RuntimeException>) context -> {
            return jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_CREATE_PARENT_CHILD_RELATIONS);
                ps.setString(1, parentName.toString());
                ps.setString(2, childName.toString());
                ps.setString(3, type);
                ps.setString(4, childName.toString());
                ps.setString(5, childName.toString());
                return ps;
            });
        });

        if (count == 0) {
            final ParentInfo parentParentInfo = getParent(parentName);
            final ParentInfo childParentInfo = getParent(childName);
            if (parentParentInfo != null) {
                throw new RuntimeException("Cannot create a child table as parent "
                    + "- parent table: " + parentName
                    + " already have a parent table = " + parentParentInfo.getName());
            } else if (childParentInfo != null) {
                throw new RuntimeException("Cannot have a child table having more than one parent "
                    + "- Child Table: " + childName
                    + " already have a parent Table=" + childParentInfo.getName());
            } else {
                throw new RuntimeException("Unknown Error - Fail to create parent child relation");
            }
        }
    }

    @Override
    public void rename(final QualifiedName oldName, final QualifiedName newName) {
        try {
            retryTemplate.execute((RetryCallback<Void, RuntimeException>) context -> {
                jdbcTemplate.update(connection -> {
                    final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_ENTITY);
                    ps.setString(1, newName.toString());
                    ps.setString(2, newName.toString());
                    ps.setString(3, oldName.toString());
                    ps.setString(4, oldName.toString());
                    return ps;
                });
                return null;
            });
        } catch (RuntimeException e) {
            log.error("Failed to rename entity", e);
            throw e;
        }
    }

    @Override
    public void drop(final QualifiedName name) {
        try {
            retryTemplate.execute((RetryCallback<Void, RuntimeException>) context -> {
                final int count = jdbcTemplate.update(connection -> {
                    final PreparedStatement ps = connection.prepareStatement(SQL_DROP_ENTITY);
                    ps.setString(1, name.toString());
                    ps.setString(2, name.toString());
                    return ps;
                });

                if (count == 0) {
                    throw new RuntimeException("Fail to drop " + name.toString() + " because it still has child table");
                }
                return null;
            });
        } catch (RuntimeException e) {
            log.error("Failed to drop entity", e);
            throw e;
        }
    }

    @Override
    public ParentInfo getParent(final QualifiedName name) {
        return retryTemplate.execute((RetryCallback<ParentInfo, RuntimeException>) context -> {
            final List<ParentInfo> parents = jdbcTemplate.query(
                SQL_GET_PARENTS, new Object[]{name.toString()}, (rs, rowNum) -> {
                    final ParentInfo parentInfo = new ParentInfo();
                    parentInfo.setName(rs.getString("parent"));
                    parentInfo.setRelationType(rs.getString("relation_type"));
                    return parentInfo;
                });
            return parents.isEmpty() ? null : parents.get(0);
        });
    }

    @Override
    public Set<ChildInfo> getChildren(final QualifiedName name) {
        return retryTemplate.execute((RetryCallback<Set<ChildInfo>, RuntimeException>) context -> {
            final List<ChildInfo> children = jdbcTemplate.query(
                SQL_GET_CHILDREN, new Object[]{name.toString()}, (rs, rowNum) -> {
                    final ChildInfo childInfo = new ChildInfo();
                    childInfo.setName(rs.getString("child"));
                    childInfo.setRelationType(rs.getString("relation_type"));
                    return childInfo;
                });
            return new HashSet<>(children);
        });
    }
}
