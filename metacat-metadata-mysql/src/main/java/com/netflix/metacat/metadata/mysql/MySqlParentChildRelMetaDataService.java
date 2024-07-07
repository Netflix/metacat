package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.ChildInfoDto;
import com.netflix.metacat.common.dto.ParentInfoDto;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.properties.ParentChildRelationshipProperties;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringJoiner;
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

    static final String SQL_IS_PARENT_TABLE = "SELECT 1 FROM parent_child_relation WHERE parent = ?";
    static final String SQL_IS_CHILD_TABLE = "SELECT 1 FROM parent_child_relation WHERE child = ?";

    static final String SQL_GET_PARENT_UUIDS = "SELECT DISTINCT parent_uuid FROM parent_child_relation "
        + "where parent = ?";

    static final String SQL_GET_CHILDREN_UUIDS = "SELECT DISTINCT child_uuid FROM parent_child_relation "
        + "where child = ?";

    static final String SQL_GET_CHILDREN_SIZE_PER_REL = "SELECT COUNT(*) FROM parent_child_relation "
        + "where parent = ? and relation_type = ?";

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

    private Integer getMaxAllowedFromNestedMap(
        final QualifiedName parent,
        final String relationType,
        final Map<String, Map<String, Integer>> maxAllowPerResourcePerRelType,
        final boolean isTable) {
        Integer maxCloneAllow = null;
        final Map<String, Integer> maxAllowPerResource = maxAllowPerResourcePerRelType.get(relationType);
        if (maxAllowPerResource != null) {
            if (isTable) {
                maxCloneAllow = maxAllowPerResource.get(parent.toString());
            } else {
                maxCloneAllow = maxAllowPerResource.get(parent.getDatabaseName());
            }
        }
        return maxCloneAllow;
    }

    private void validateMaxAllow(final QualifiedName parentName,
                                  final String type,
                                  final ParentChildRelationshipProperties props) {
        // Validate max clone allow
        // First check if the parent table have configured max allowed on the table config
        Integer maxAllow = getMaxAllowedFromNestedMap(
            parentName,
            type,
            props.getMaxAllowPerTablePerRelType(),
            true
        );

        // Then check if the parent have configured max allowed on the db config
        if (maxAllow == null) {
            maxAllow = getMaxAllowedFromNestedMap(
                parentName,
                type,
                props.getMaxAllowPerDBPerRelType(),
                false
            );
        }

        // If not specified in maxAllowPerDBPerRelType,check the default max Allow based on relationType
        if (maxAllow == null) {
            final Integer count = props.getDefaultMaxAllowPerRelType().get(type);
            if (count != null) {
                maxAllow = count;
            }
        }

        // Finally fallback to the default value for all types
        if (maxAllow == null) {
            maxAllow = props.getMaxAllow();
        }

        // if maxAllow < 0, this means we can create as many child table under the parent
        if (maxAllow < 0) {
            return;
        }

        if (getChildrenCountPerType(parentName, type) >= maxAllow) {
            final String errorMsg = String.format(
                "Parent table: %s is not allow to have more than %s child table for %s relation type",
                parentName, maxAllow, type);
            throw new ParentChildRelServiceException(errorMsg);
        }
    }

    private void validateCreate(final QualifiedName parentName,
                                final String parentUUID,
                                final QualifiedName childName,
                                final String childUUID,
                                final String type,
                                final ParentChildRelationshipProperties props) {
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

        // Validation to prevent creating a parent with an uuid that is different from the existing parent uuids
        final Set<String> existingParentUuids = getExistingUUIDS(parentName.toString());
        validateUUIDs(parentName.toString(), existingParentUuids, parentUUID, "Parent");

        // Validation to prevent creating a child with an uuid that is different from the existing child uuids
        final Set<String> existingChildUuids = getExistingUUIDS(childName.toString());
        validateUUIDs(childName.toString(), existingChildUuids, childUUID, "Child");

        // Validation to control how many children tables can be created per type
        validateMaxAllow(parentName, type, props);
    }

    @Override
    public void createParentChildRelation(final QualifiedName parentName,
                                          final String parentUUID,
                                          final QualifiedName childName,
                                          final String childUUID,
                                          final String type,
                                          final ParentChildRelationshipProperties props) {
        validateCreate(parentName, parentUUID, childName, childUUID, type, props);
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
    public void rename(final QualifiedName oldName, final QualifiedName newName) {
        if (isChildTable(newName)) {
            throw new ParentChildRelServiceException(newName + " is already a child table");
        }
        if (isParentTable(newName)) {
            throw new ParentChildRelServiceException(newName + " is already a parent table");
        }

        renameParent(oldName, newName);
        renameChild(oldName, newName);
        log.info("Successfully rename parent child relationship for oldName={}, newName={}",
            oldName, newName
        );
    }

    private void renameParent(final QualifiedName oldName, final QualifiedName newName) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_PARENT_ENTITY);
                ps.setString(1, newName.toString());
                ps.setString(2, oldName.toString());

                return ps;
            });
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to rename parent from oldName:" + oldName + " to " + newName
                + " in MySqlParentChildRelMetadataService", e);
        }
    }

    private void renameChild(final QualifiedName oldName, final QualifiedName newName) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_RENAME_CHILD_ENTITY);
                ps.setString(1, newName.toString());
                ps.setString(2, oldName.toString());
                return ps;
            });
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to rename child from oldName:" + oldName + " to " + newName
                + " in MySqlParentChildRelMetadataService", e);
        }
    }

    @Override
    public void drop(final QualifiedName name) {
        dropParent(name);
        dropChild(name);
        log.info("Successfully drop parent child relationship for name={}", name);
    }

    private void dropParent(final QualifiedName name) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_DROP_PARENT);
                ps.setString(1, name.toString());
                return ps;
            });
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to drop parent:" + name
                + " in MySqlParentChildRelMetadataService", e);
        }
    }

    private void dropChild(final QualifiedName name) {
        try {
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_DROP_CHILD);
                ps.setString(1, name.toString());
                return ps;
            });
        } catch (Exception e) {
            throw new ParentChildRelServiceException("Fail to drop child:" + name
                + " in MySqlParentChildRelMetadataService", e);
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

    @Override
    public Set<ParentInfoDto> getParentsDto(final QualifiedName name) {
        return getParents(name).stream()
            .map(converterUtil::toParentInfoDto).collect(Collectors.toSet());
    }

    @Override
    public boolean isParentTable(final QualifiedName tableName) {
        return tableExist(tableName.toString(), SQL_IS_PARENT_TABLE);
    }

    @Override
    public boolean isChildTable(final QualifiedName tableName) {
        return tableExist(tableName.toString(), SQL_IS_CHILD_TABLE);
    }

    private boolean tableExist(final String tableName, final String sql) {
        return jdbcTemplate.query(sql,
            new PreparedStatementSetter() {
                @Override
                public void setValues(final PreparedStatement ps) throws SQLException {
                    ps.setString(1, tableName);
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

    private Set<String> getExistingUUIDS(final String tableName) {
        final Set<String> existingUUIDs = new HashSet<>();
        existingUUIDs.addAll(getParentUuidsForParent(tableName));
        existingUUIDs.addAll(getChildUuidsForChild(tableName));
        return existingUUIDs;
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

    private void validateUUIDs(final String name,
                               final Set<String> existingUUIDs,
                               final String inputUUID,
                               final String entity
    ) throws ParentChildRelServiceException {
        if (existingUUIDs.size() > 1 || (!existingUUIDs.isEmpty() && !existingUUIDs.contains(inputUUID))) {
            final StringJoiner uuidJoiner = new StringJoiner(", ");
            for (String uuid : existingUUIDs) {
                uuidJoiner.add(uuid);
            }
            final String existingUuidsString = uuidJoiner.toString();

            throw new ParentChildRelServiceException(
                String.format("Cannot create parent-child relation: %s '%s' has existing UUIDs [%s] "
                        + "that differ from the input %s UUID '%s'. This normally means table %s already exists",
                    entity, name, existingUuidsString, entity, inputUUID, name)
            );
        }
    }

    private int getChildrenCountPerType(final QualifiedName parent, final String type) {
        final List<Object> params = new ArrayList<>();
        params.add(parent.toString());
        params.add(type);
        return jdbcTemplate.queryForObject(SQL_GET_CHILDREN_SIZE_PER_REL, params.toArray(), Integer.class);
    }
}
