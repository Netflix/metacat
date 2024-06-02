package com.netflix.metacat

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.model.ChildInfo
import com.netflix.metacat.common.server.model.ParentInfo
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService
import com.netflix.metacat.metadata.mysql.MySqlParentChildRelMetaDataService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement

class ParentChildRelMetadataServiceSpec extends Specification{
    @Shared
    private ParentChildRelMetadataService service;
    @Shared
    private JdbcTemplate jdbcTemplate;

    @Shared
    private String catalog = "prodhive"
    @Shared
    private String database = "testpc"

    @Shared
    private static final String SQL_CREATE_PARENT_CHILD_RELATIONS =
        "INSERT INTO parent_child_relation (parent, parent_uuid, child, child_uuid, relation_type) " +
            "VALUES (?, ?, ?, ?, ?)"

    def setupSpec() {
        String jdbcUrl = "jdbc:mysql://localhost:3306/metacat"
        String username = "metacat_user"
        String password = "metacat_user_password"

        DriverManagerDataSource dataSource = new DriverManagerDataSource()
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver")
        dataSource.setUrl(jdbcUrl)
        dataSource.setUsername(username)
        dataSource.setPassword(password)

        jdbcTemplate = new JdbcTemplate(dataSource)
        service = new MySqlParentChildRelMetaDataService(jdbcTemplate)
    }

    def cleanup() {
        jdbcTemplate.update("DELETE FROM parent_child_relation")
    }

    void createParentChildRelation(String parentName, String parentUUID, String childName, String childUUID, String type) {
        jdbcTemplate.update { Connection connection ->
            PreparedStatement ps = connection.prepareStatement(SQL_CREATE_PARENT_CHILD_RELATIONS)
            ps.setString(1, parentName.toString())
            ps.setString(2, parentUUID)
            ps.setString(3, childName.toString())
            ps.setString(4, childUUID)
            ps.setString(5, type)
            return ps
        }
    }

    def "Test CreateThenDelete - OneChildOneParent"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def parentUUID = "p_uuid";
        def childUUID = "c_uuid";
        def type = "clone";
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        when:
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        when:
        service.deleteParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()
        assert service.getChildren(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(parent, Optional.empty()).isEmpty()

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()

    }

    def "Test Create - oneParentMultiChildren"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid";
        def child1 = QualifiedName.ofTable(catalog, database, "c1")
        def child1UUID = "c1_uuid";
        def child2 = QualifiedName.ofTable(catalog, database, "c2")
        def child2UUID = "c2_uuid";
        def type = "clone";
        def parent_children_expected  = [
            new ChildInfo(child1.toString(), type, child1UUID),
            new ChildInfo(child2.toString(), type, child2UUID),
        ] as Set
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set

        when:
        service.createParentChildRelation(parent, parentUUID, child1, child1UUID, type)
        service.createParentChildRelation(parent, parentUUID, child2, child2UUID, type)

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert parent_children_expected == service.getChildren(parent, Optional.of(parentUUID))
        assert parent_children_expected == service.getChildren(parent, Optional.empty())

        // Test Children
        // Test Child 1
        assert child_parent_expected == service.getParents(child1, Optional.of(child1UUID))
        assert child_parent_expected == service.getParents(child1, Optional.empty())
        assert service.getChildren(child1, Optional.of(child1UUID)).isEmpty()
        assert service.getChildren(child1, Optional.empty()).isEmpty()

        assert child_parent_expected == service.getParents(child2, Optional.of(child2UUID))
        assert child_parent_expected == service.getParents(child2, Optional.empty())
        assert service.getChildren(child2, Optional.of(child2UUID)).isEmpty()
        assert service.getChildren(child2, Optional.empty()).isEmpty()
    }

    def "Test Create - oneChildMultiParentException"() {
        setup:
        def parent1 = QualifiedName.ofTable(catalog, database, "p1")
        def parent1UUID = "p1_uuid";
        def parent2 = QualifiedName.ofTable(catalog, database, "p2")
        def parent2UUID = "p2_uuid";
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent1, parent1UUID, child, childUUID, type)

        when:
        service.createParentChildRelation(parent2, parent2UUID, child, childUUID, type)

        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("Cannot have a child table having more than one parent")

        // Test Child
        def child_parent_expected = [new ParentInfo(parent1.toString(), type, parent1UUID)] as Set
        assert child_parent_expected == service.getParents(child, Optional.of(childUUID))
        assert child_parent_expected == service.getParents(child, Optional.empty())

        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test Create - oneChildAsParentOfAnotherException"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def grandChild = QualifiedName.ofTable(catalog, database, "gc")
        def grandChildUUID = "gc_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)

        when:
        service.createParentChildRelation(child, childUUID, grandChild, grandChildUUID, type)

        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("Cannot create a child table as parent")

        // Test Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test Create - oneParentAsChildOfAnother"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def grandChild = QualifiedName.ofTable(catalog, database, "gc")
        def grandChildUUID = "gc_uuid"
        def type = "clone"
        service.createParentChildRelation(child, childUUID, grandChild, grandChildUUID, type)

        when:
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("Cannot create a parent table on top of another parent")
    }

    def "Test Rename Parent - One Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone";
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)

        when:
        service.rename(parent, newParent, Optional.of(parentUUID))

        then:
        // Test Old Parent Name
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()
        assert service.getChildren(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(parent, Optional.empty()).isEmpty()

        // Test New Parent Name
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()
        def newParent_children_expected = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert service.getChildren(newParent, Optional.of(parentUUID)) == newParent_children_expected
        assert service.getChildren(newParent, Optional.empty()) == newParent_children_expected

        // Test Child
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child, Optional.of(childUUID))
        assert child_parent_expected == service.getParents(child, Optional.empty())
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        // rename back
        when:
        service.rename(newParent, parent, Optional.of(parentUUID))
        child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set

        then:
        // Test new Parent Name
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()
        assert service.getChildren(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(newParent, Optional.empty()).isEmpty()

        // Test old Parent Name
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()
        assert service.getChildren(parent, Optional.of(parentUUID)) == newParent_children_expected
        assert service.getChildren(parent, Optional.empty()) == [new ChildInfo(child.toString(), type, childUUID)] as Set

        // Test Child
        assert child_parent_expected == service.getParents(child, Optional.of(childUUID))
        assert child_parent_expected == service.getParents(child, Optional.empty())
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test Rename Parent - Multi Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child1 = QualifiedName.ofTable(catalog, database, "c1")
        def child1UUID = "c1_uuid"
        def child2 = QualifiedName.ofTable(catalog, database, "c2")
        def child2UUID = "c2_uuid"
        def type = "clone";
        service.createParentChildRelation(parent, parentUUID, child1, child1UUID, type)
        service.createParentChildRelation(parent, parentUUID, child2, child2UUID, type)
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set

        when:
        service.rename(parent, newParent, Optional.of(parentUUID))

        then:
        // Test Child1
        assert service.getParents(child1, Optional.of(child1UUID)) == child_parent_expected
        assert service.getParents(child1, Optional.empty()) == child_parent_expected
        //Test Child2
        assert service.getParents(child2, Optional.of(child2UUID)) == child_parent_expected
        assert service.getParents(child2, Optional.empty()) == child_parent_expected
    }

    def "Test Rename Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        service.rename(child, newChild, Optional.of(childUUID))
        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        def parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID)] as Set
        assert parent_children_expected == service.getChildren(parent, Optional.of(parentUUID))
        assert parent_children_expected == service.getChildren(parent, Optional.empty())

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        // Test New Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(newChild, Optional.of(childUUID))
        assert child_parent_expected == service.getParents(newChild, Optional.empty())
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        // rename back
        when:
        service.rename(newChild, child, Optional.of(childUUID))
        parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert parent_children_expected == service.getChildren(parent, Optional.of(parentUUID))
        assert parent_children_expected == service.getChildren(parent, Optional.empty())

        // Test New Child
        assert service.getParents(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()).isEmpty()
        assert service.getChildren(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(newChild, Optional.empty()).isEmpty()

        // Test Child
        assert child_parent_expected == service.getParents(child, Optional.of(childUUID))
        assert child_parent_expected == service.getParents(child, Optional.empty())
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }


    def "Test Drop Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone";
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        when:
        service.drop(child, Optional.of(childUUID))

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()
        assert service.getChildren(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(parent, Optional.empty()).isEmpty()

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test CreateThenRenameThenDropWithUUIDSpecified- two same parent name but different id"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        def parentUUID = "p_uuid";
        def parentUUID2 = "p_uuid_2"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid";
        def type = "clone";

        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID), new ParentInfo(parent.toString(), type, parentUUID2)] as Set
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        when:
        createParentChildRelation(parent.toString(), parentUUID, child.toString(), childUUID, type)
        createParentChildRelation(parent.toString(), parentUUID2, child.toString(), childUUID, type)

        then:
        // Test Parent with UUID1
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Parent with UUID2
        assert service.getParents(parent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID2)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        when:
        service.rename(parent, newParent, Optional.of(parentUUID))
        // We only do rename on the specific id = parentUUID so the same parent name with uuid2 should not be changed
        child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID), new ParentInfo(parent.toString(), type, parentUUID2)] as Set

        then:
        // Test Parent with UUID1
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()

        assert service.getChildren(newParent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(newParent, Optional.empty()) == parent_children_expected

        // Test Parent with UUID2
        assert service.getParents(parent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID2)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        // delete the record with parentUUID2
        when:
        service.drop(parent, Optional.of(parentUUID2))
        child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set

        then:
        // Test Parent with uuid1
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()
        assert service.getChildren(newParent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(newParent, Optional.empty()) == parent_children_expected

        // Test Parent with UUID2
        assert service.getParents(parent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()
        assert service.getChildren(parent, Optional.of(parentUUID2)).isEmpty()
        assert service.getChildren(parent, Optional.empty()).isEmpty()

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test CreateThenRenameThenDeleteWithNoIDSpecified- two same parent name but different id"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        def parentUUID = "p_uuid";
        def parentUUID2 = "p_uuid_2"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid";
        def type = "clone";

        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID), new ParentInfo(parent.toString(), type, parentUUID2)] as Set
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        when:
        createParentChildRelation(parent.toString(), parentUUID, child.toString(), childUUID, type)
        createParentChildRelation(parent.toString(), parentUUID2, child.toString(), childUUID, type)

        then:
        // Test Parent with UUID1
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Parent with UUID2
        assert service.getParents(parent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID2)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        when:
        service.rename(parent, newParent, Optional.empty())
        // We only do rename on the specific id = parentUUID so the same parent name with uuid2 should not be changed
        child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID), new ParentInfo(newParent.toString(), type, parentUUID2)] as Set

        then:
        // Test Parent with UUID1
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()

        assert service.getChildren(newParent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(newParent, Optional.empty()) == parent_children_expected

        // Test Parent with UUID2
        assert service.getParents(newParent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()

        assert service.getChildren(newParent, Optional.of(parentUUID2)) == parent_children_expected
        assert service.getChildren(newParent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()

        // drop based only on the parentName
        when:
        service.drop(newParent, Optional.empty())

        then:
        // Test Parent with uuid1
        assert service.getParents(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()
        assert service.getChildren(newParent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(newParent, Optional.empty()).isEmpty()

        // Test Parent with UUID2
        assert service.getParents(newParent, Optional.of(parentUUID2)).isEmpty()
        assert service.getParents(newParent, Optional.empty()).isEmpty()
        assert service.getChildren(newParent, Optional.of(parentUUID2)).isEmpty()
        assert service.getChildren(newParent, Optional.empty()).isEmpty()

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()
        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getChildren(child, Optional.empty()).isEmpty()
    }

    def "Test CreateThenRenameThenDropWithIDSpecified - two same child name but different child id"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def newChild = QualifiedName.ofTable(catalog, database, "nc")
        def parentUUID = "p_uuid";
        def childUUID = "c_uuid";
        def childUUID2 = "c_uuid_2"
        def type = "clone";
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID), new ChildInfo(child.toString(), type, childUUID2)] as Set
        // fake a record in the db with the same child name but different child uuid
        createParentChildRelation(parent.toString(), parentUUID, child.toString(), childUUID2, type)

        when:
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        // Test Child2
        assert service.getParents(child, Optional.of(childUUID2)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        when:
        service.rename(child, newChild, Optional.of(childUUID))
        parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID), new ChildInfo(child.toString(), type, childUUID2)] as Set

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(newChild, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        assert service.getChildren(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        // Test Child2
        assert service.getParents(child, Optional.of(childUUID2)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        when:
        service.drop(child, Optional.of(childUUID2))
        parent_children_expected = [new ChildInfo(newChild.toString(), type, childUUID)] as Set

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(newChild, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        assert service.getChildren(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        // Test Child2
        assert service.getParents(child, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()

        assert service.getChildren(child, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(child, Optional.empty()).isEmpty()
    }

    def "Test CreateThenRenameThenDropWithNoIDSpecified - two same child name but different child id"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def newChild = QualifiedName.ofTable(catalog, database, "nc")
        def parentUUID = "p_uuid";
        def childUUID = "c_uuid";
        def childUUID2 = "c_uuid_2"
        def type = "clone";
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID), new ChildInfo(child.toString(), type, childUUID2)] as Set
        // fake a record in the db with the same child name but different child uuid
        createParentChildRelation(parent.toString(), parentUUID, child.toString(), childUUID2, type)

        when:
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(child, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        // Test Child2
        assert service.getParents(child, Optional.of(childUUID2)) == child_parent_expected
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        assert service.getChildren(child, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(child, Optional.empty()) == child_parent_expected

        when:
        service.rename(child, newChild, Optional.empty())
        parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID), new ChildInfo(newChild.toString(), type, childUUID2)] as Set

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)) == parent_children_expected
        assert service.getChildren(parent, Optional.empty()) == parent_children_expected

        // Test Child
        assert service.getParents(newChild, Optional.of(childUUID)) == child_parent_expected
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        assert service.getChildren(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        // Test Child2
        assert service.getParents(newChild, Optional.of(childUUID2)) == child_parent_expected
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        assert service.getChildren(newChild, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(newChild, Optional.empty()) == child_parent_expected

        when:
        service.drop(newChild, Optional.empty())

        then:
        // Test Parent
        assert service.getParents(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getParents(parent, Optional.empty()).isEmpty()

        assert service.getChildren(parent, Optional.of(parentUUID)).isEmpty()
        assert service.getChildren(parent, Optional.empty()).isEmpty()

        // Test Child
        assert service.getParents(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()).isEmpty()

        assert service.getChildren(newChild, Optional.of(childUUID)).isEmpty()
        assert service.getParents(newChild, Optional.empty()).isEmpty()

        // Test Child2
        assert service.getParents(newChild, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(newChild, Optional.empty()).isEmpty()

        assert service.getChildren(newChild, Optional.of(childUUID2)).isEmpty()
        assert service.getParents(newChild, Optional.empty()).isEmpty()
    }
}
