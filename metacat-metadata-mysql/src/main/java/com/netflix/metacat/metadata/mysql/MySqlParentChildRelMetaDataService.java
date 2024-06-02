package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Parent Child Relationship Metadata Service.
 * This stores the parent child relationship of two entities as first class citizen in Metacat.
 */
@Slf4j
@SuppressFBWarnings
@Transactional("metadataTxManager")
public class MySqlParentChildRelMetaDataService implements ParentChildRelMetadataService {
    static final String SQL_CREATE_PARENT_CHILD_RELATIONS =
        "INSERT INTO parent_child_relation (parent, parent_uuid, child, child_uuid, relation_type) "
            + "VALUES (?, ?, ?, ?, ?)";

    static final String SQL_DELETE_PARENT_CHILD_RELATIONS =
        "DELETE FROM parent_child_relation "
            + "WHERE parent = ? AND parent_uuid = ? AND child = ? AND child_uuid = ? AND relation_type = ?";

    static final String SQL_RENAME_PARENT_ENTITY = "UPDATE parent_child_relation "
        + "SET parent = ? WHERE parent = ?";
    static final String SQL_RENAME_CHILD_ENTITY = "UPDATE parent_child_relation "
        + "SET child = ? WHERE child = ?";

    static final String SQL_DROP_CHILD = "DELETE FROM parent_child_relation "
        + "WHERE child = ? ";
    static final String SQL_DROP_PARENT = "DELETE FROM parent_child_relation "
        + "WHERE parent = ? ";

    static final String SQL_GET_PARENTS = "SELECT parent, parent_uuid, relation_type "
        + "FROM parent_child_relation WHERE child = ?";

    static final String SQL_GET_CHILDREN = "SELECT child, child_uuid, relation_type "
        + "FROM parent_child_relation WHERE parent = ?";

    private JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     *
     * @param jdbcTemplate jdbc template
     */
    @Autowired
    public MySqlParentChildRelMetaDataService(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void createParentChildRelation(final QualifiedName parentName,
                                          final String parentUUID,
                                          final QualifiedName childName,
                                          final String childUUID,
                                          final String type) {
        // Validation to prevent having a child have two parents
        final Set<ParentInfo> childParents = getParents(childName);
        if (!childParents.isEmpty()) {
            throw new RuntimeException("Cannot have a child table having more than one parent "
                + "- Child Table: " + childName
                + " already have a parent Table=" + childParents.stream().findFirst().get());
        }

        // Validation to prevent creating a child table as a parent of another child table
        final Set<ParentInfo> parentParents = getParents(parentName);
        if (!parentParents.isEmpty()) {
            throw new RuntimeException("Cannot create a child table as parent "
                + "- parent table: " + parentName
                + " already have a parent table = " + parentParents.stream().findFirst().get());
        }

        // Validation to prevent creating a parent on top of a table that have children
        final Set<ChildInfo> childChildren = getChildren(childName);
        if (!childChildren.isEmpty()) {
            throw new RuntimeException("Cannot create a parent table on top of another parent "
                + "- child table: " + childName + " already have child");
        }

        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_CREATE_PARENT_CHILD_RELATIONS);
            ps.setString(1, parentName.toString());
            ps.setString(2, parentUUID);
            ps.setString(3, childName.toString());
            ps.setString(4, childUUID);
            ps.setString(5, type);
            return ps;
        });
        log.info("Successfully create parent child relationship with parent={}, parentUUID={}, "
                + "child={}, childUUID={}, relationType = {}",
            parentName, parentUUID, childName, childUUID, type
        );
    }

    @Override
    public void deleteParentChildRelation(final QualifiedName parentName,
                                          final String parentUUID,
                                          final QualifiedName childName,
                                          final String childUUID,
                                          final String type) {
        log.info("Deleting parent child relationship with parent={}, parentUUID={}, "
                + "child={}, childUUID={}, relationType = {}",
            parentName, parentUUID, childName, childUUID, type
        );
        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_DELETE_PARENT_CHILD_RELATIONS);
            ps.setString(1, parentName.toString());
            ps.setString(2, parentUUID);
            ps.setString(3, childName.toString());
            ps.setString(4, childUUID);
            ps.setString(5, type);
            return ps;
        });
        log.info("Successfully delete parent child relationship with parent={}, parentUUID={}, "
                + "child={}, childUUID={}, relationType = {}",
            parentName, parentUUID, childName, childUUID, type
        );
    }

    @Override
    public void rename(final QualifiedName oldName, final QualifiedName newName) {
        try {
            renameParent(oldName, newName);
            renameChild(oldName, newName);
            log.info("Successfully rename parent child relationship for oldName={}, newName={}",
                oldName, newName
            );
        } catch (RuntimeException e) {
            log.error("Failed to rename entity", e);
            throw e;
        }
    }

    private void renameParent(final QualifiedName oldName, final QualifiedName newName) {
        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_PARENT_ENTITY);
            ps.setString(1, newName.toString());
            ps.setString(2, oldName.toString());

            return ps;
        });
    }

    private void renameChild(final QualifiedName oldName, final QualifiedName newName) {
        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_CHILD_ENTITY);
            ps.setString(1, newName.toString());
            ps.setString(2, oldName.toString());
            return ps;
        });
    }

    @Override
    public void drop(final QualifiedName name) {
        dropParent(name);
        dropChild(name);
        log.info("Successfully drop parent child relationship for name={}, uuid={}", name);
    }

    private void dropParent(final QualifiedName name) {
        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_DROP_PARENT);
            ps.setString(1, name.toString());
            return ps;
        });
    }

    private void dropChild(final QualifiedName name) {
        jdbcTemplate.update(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_DROP_CHILD);
            ps.setString(1, name.toString());
            return ps;
        });
    }

    @Override
    public Set<ParentInfo> getParents(final QualifiedName name) {
        final List<Object> params = new ArrayList<>();
        params.add(name.toString());
        final List<ParentInfo> parents = jdbcTemplate.query(
            SQL_GET_PARENTS, params.toArray(), (rs, rowNum) -> {
                final ParentInfo parentInfo = new ParentInfo();
                parentInfo.setName(rs.getString("parent"));
                parentInfo.setRelationType(rs.getString("relation_type"));
                parentInfo.setUuid(rs.getString("parent_uuid"));
                return parentInfo;
            });
        return new HashSet<>(parents);
    }

    @Override
    public Set<ChildInfo> getChildren(final QualifiedName name) {
        final List<Object> params = new ArrayList<>();
        params.add(name.toString());
        final List<ChildInfo> children = jdbcTemplate.query(
            SQL_GET_CHILDREN, params.toArray(), (rs, rowNum) -> {
                final ChildInfo childInfo = new ChildInfo();
                childInfo.setName(rs.getString("child"));
                childInfo.setRelationType(rs.getString("relation_type"));
                childInfo.setUuid(rs.getString("child_uuid"));
                return childInfo;
            });
        return new HashSet<>(children);
    }
}
