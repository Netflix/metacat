package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.notifications.ChildInfoDto;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.Set;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.stream.Collectors;

import java.sql.ResultSet;
import java.sql.SQLException;

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
    static final String SQL_RENAME_SOFT_PARENT_INSERT =
        "INSERT INTO parent_child_relation (parent, parent_uuid, child, child_uuid, relation_type) "
            + "SELECT ?, parent_uuid, child, child_uuid, relation_type "
            + "FROM parent_child_relation "
            + "WHERE parent = ?";
    static final String SQL_RENAME_SOFT_CHILD_INSERT =
        "INSERT INTO parent_child_relation (parent, parent_uuid, child, child_uuid, relation_type) "
            + "SELECT parent, parent_uuid, ?, child_uuid, relation_type "
            + "FROM parent_child_relation "
            + "WHERE child = ?";

    static final String SQL_CHECK_TABLE_EXIST =
        "SELECT 1 FROM parent_child_relation WHERE parent = ? OR child = ? LIMIT 1";

    static final String SQL_GET_PARENT_UUIDS = "SELECT DISTINCT parent_uuid FROM parent_child_relation "
        + "where parent = ?";

    static final String SQL_GET_CHILDREN_UUIDS = "SELECT DISTINCT child_uuid FROM parent_child_relation "
        + "where child = ?";

    private final JdbcTemplate jdbcTemplate;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param jdbcTemplate jdbc template
     * @param converterUtil converterUtil
     */
    @Autowired
    public MySqlParentChildRelMetaDataService(final JdbcTemplate jdbcTemplate, final ConverterUtil converterUtil) {
        this.jdbcTemplate = jdbcTemplate;
        this.converterUtil = converterUtil;
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
            throw new ParentChildRelServiceException("Cannot have a child table having more than one parent "
                + "- Child Table: " + childName
                + " already have a parent Table=" + childParents.stream().findFirst().get());
        }

        // Validation to prevent creating a child table as a parent of another child table
        final Set<ParentInfo> parentParents = getParents(parentName);
        if (!parentParents.isEmpty()) {
            throw new ParentChildRelServiceException("Cannot create a child table as parent "
                + "- parent table: " + parentName
                + " already have a parent table = " + parentParents.stream().findFirst().get());
        }

        // Validation to prevent creating a parent on top of a table that have children
        final Set<ChildInfo> childChildren = getChildren(childName);
        if (!childChildren.isEmpty()) {
            throw new ParentChildRelServiceException("Cannot create a parent table on top of another parent "
                + "- child table: " + childName + " already have child");
        }

        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_CREATE_PARENT_CHILD_RELATIONS);
                ps.setString(1, parentName.toString());
                ps.setString(2, parentUUID);
                ps.setString(3, childName.toString());
                ps.setString(4, childUUID);
                ps.setString(5, type);
                return ps;
            });
        } catch (Exception e) {
            log.error("Fail to create parent child relationship with parent={}, parentUUID={}, "
                    + "child={}, childUUID={}, relationType = {}",
                parentName, parentUUID, childName, childUUID, type
            );
            throw new ParentChildRelServiceException("Fail to create parent child relationship "
                + "for child table:" + childName + " under parent table:" + parentName, e);
        }
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

        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_DELETE_PARENT_CHILD_RELATIONS);
                ps.setString(1, parentName.toString());
                ps.setString(2, parentUUID);
                ps.setString(3, childName.toString());
                ps.setString(4, childUUID);
                ps.setString(5, type);
                return ps;
            });
        } catch (Exception e) {
            log.error("Fail to delete parent child relationship with parent={}, parentUUID={}, "
                    + "child={}, childUUID={}, relationType = {}",
                parentName, parentUUID, childName, childUUID, type
            );
            throw new ParentChildRelServiceException("Fail to delete parent child relationship"
                + " for child table:" + childName + " under parent table:" + parentName, e);
        }
        log.info("Successfully delete parent child relationship with parent={}, parentUUID={}, "
                + "child={}, childUUID={}, relationType = {}",
            parentName, parentUUID, childName, childUUID, type
        );
    }

    @Override
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Pair<Set<String>, Set<String>> rename(final QualifiedName oldName, final QualifiedName newName) {
        if (tableExist(newName.toString())) {
            throw new ParentChildRelServiceException(String.format(
                "Fail to rename table from %s to %s: possibly %s already exists",
                oldName, newName, newName));
        }

        final Set<String> parentUuids = renameParent(oldName, newName);
        final Set<String> childUuids = renameChild(oldName, newName);

        if (!parentUuids.isEmpty() || !childUuids.isEmpty()) {
            log.info("Successfully rename parent child relationship for oldName={}, newName={}",
                oldName, newName
            );
        }
        return new ImmutablePair<>(parentUuids, childUuids);
    }

    private Set<String> renameParent(final QualifiedName oldName, final QualifiedName newName) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_SOFT_PARENT_INSERT);
                ps.setString(1, newName.toString());
                ps.setString(2, oldName.toString());

                return ps;
            });
            return getParentUuidsForParent(newName.toString());
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to rename parent from oldName:" + oldName + " to " + newName
                + " in MySqlParentChildRelMetadataService", e);
        }
    }

    private Set<String> renameChild(final QualifiedName oldName, final QualifiedName newName) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_SOFT_CHILD_INSERT);
                ps.setString(1, newName.toString());
                ps.setString(2, oldName.toString());
                return ps;
            });
            return getChildUuidsForChild(newName.toString());
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to rename child from oldName:" + oldName + " to " + newName
                + " in MySqlParentChildRelMetadataService", e);
        }
    }

    @Override
    public void drop(final QualifiedName name, final Optional<Pair<Set<String>, Set<String>>> uuids) {
        Optional<Set<String>> parentUUIDs = Optional.empty();
        Optional<Set<String>> childUUIDs = Optional.empty();

        if (uuids.isPresent()) {
            parentUUIDs = Optional.of(uuids.get().getLeft());
            childUUIDs = Optional.of(uuids.get().getRight());
        }

        if (drop(name, parentUUIDs, SQL_DROP_PARENT) > 0 || drop(name, childUUIDs, SQL_DROP_CHILD) > 0) {
            log.info("Successfully drop parent child relationship for name={} and parentUUIDs = {} and childUUIDs = {}",
                name, parentUUIDs, childUUIDs);
        }
    }

    private int drop(final QualifiedName name, final Optional<Set<String>> uuids, final String sql) {
        final String sqlWithInClause;
        final String uuidColumn = sql.equals(SQL_DROP_PARENT) ? "parent_uuid" : "child_uuid";

        if (uuids.isPresent() && !uuids.get().isEmpty()) {
            final String inClause = uuids.get().stream()
                .map(uuid -> "?")
                .collect(Collectors.joining(", "));
            sqlWithInClause = sql + " AND " + uuidColumn + " IN (" + inClause + ")";
        } else {
            sqlWithInClause = sql;
        }

        try {
            return jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(sqlWithInClause);
                ps.setString(1, name.toString());

                if (uuids.isPresent()) {
                    int i = 1;
                    for (String uuid : uuids.get()) {
                        ps.setString(++i, uuid);
                    }
                }

                return ps;
            });
        } catch (Exception e) {
            if (sql.equals(SQL_DROP_PARENT)) {
                throw new ParentChildRelServiceException("Fail to drop parent:" + name
                    + " in MySqlParentChildRelMetadataService", e);
            } else {
                throw new ParentChildRelServiceException("Fail to drop child:" + name
                    + " in MySqlParentChildRelMetadataService", e);
            }
        }
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

    @Override
    public Set<ChildInfoDto> getChildrenDto(final QualifiedName name) {
        return getChildren(name).stream()
            .map(converterUtil::toChildInfoDto).collect(Collectors.toSet());
    }

    private boolean tableExist(final String tableName) {
        return jdbcTemplate.query(SQL_CHECK_TABLE_EXIST,
            new PreparedStatementSetter() {
                @Override
                public void setValues(final PreparedStatement ps) throws SQLException {
                    ps.setString(1, tableName);
                    ps.setString(2, tableName);
                }
            },
            new ResultSetExtractor<Boolean>() {
                @Override
                public Boolean extractData(final ResultSet rs) throws SQLException {
                    return rs.next();
                }
            }
        );
    }

    private Set<String> getParentUuidsForParent(final String parentName) {
        return new HashSet<>(jdbcTemplate.query(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_GET_PARENT_UUIDS);
            ps.setString(1, parentName);
            return ps;
        }, (rs, rowNum) -> rs.getString("parent_uuid")));
    }

    private Set<String> getChildUuidsForChild(final String childName) {
        return new HashSet<>(jdbcTemplate.query(connection -> {
            final PreparedStatement ps = connection.prepareStatement(SQL_GET_CHILDREN_UUIDS);
            ps.setString(1, childName);
            return ps;
        }, (rs, rowNum) -> rs.getString("child_uuid")));
    }
}
