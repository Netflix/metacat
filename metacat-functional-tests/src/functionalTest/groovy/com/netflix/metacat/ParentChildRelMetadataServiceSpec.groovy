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
        assert service.getParents(parent).isEmpty()
        assert service.getParents(parent).isEmpty()

        assert service.getChildren(parent) == parent_children_expected
        assert service.getChildren(parent) == parent_children_expected

        // Test Child
        assert service.getParents(child) == child_parent_expected
        assert service.getParents(child) == child_parent_expected

        assert service.getChildren(child).isEmpty()
        assert service.getParents(child) == child_parent_expected

        when:
        service.deleteParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert service.getParents(child).isEmpty()

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
        assert service.getParents(parent).isEmpty()
        assert parent_children_expected == service.getChildren(parent)
        assert parent_children_expected == service.getChildren(parent)

        // Test Children
        // Test Child 1
        assert child_parent_expected == service.getParents(child1)
        assert child_parent_expected == service.getParents(child1)
        assert service.getChildren(child1).isEmpty()
        assert service.getChildren(child1).isEmpty()

        assert child_parent_expected == service.getParents(child2)
        assert child_parent_expected == service.getParents(child2)
        assert service.getChildren(child2).isEmpty()
        assert service.getChildren(child2).isEmpty()
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
        assert child_parent_expected == service.getParents(child)
        assert child_parent_expected == service.getParents(child)

        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()
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
        assert service.getParents(child) == child_parent_expected
        assert service.getParents(child) == child_parent_expected

        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()
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
        service.rename(parent, newParent)

        then:
        // Test Old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test New Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getParents(newParent).isEmpty()
        def newParent_children_expected = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert service.getChildren(newParent) == newParent_children_expected
        assert service.getChildren(newParent) == newParent_children_expected

        // Test Child
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()

        // rename back
        when:
        service.rename(newParent, parent)
        child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set

        then:
        // Test new Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()

        // Test old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent) == newParent_children_expected
        assert service.getChildren(parent) == [new ChildInfo(child.toString(), type, childUUID)] as Set

        // Test Child
        assert child_parent_expected == service.getParents(child)
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()
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
        service.rename(parent, newParent)

        then:
        // Test Child1
        assert service.getParents(child1) == child_parent_expected
        assert service.getParents(child1) == child_parent_expected
        //Test Child2
        assert service.getParents(child2) == child_parent_expected
        assert service.getParents(child2) == child_parent_expected
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
        service.rename(child, newChild)
        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID)] as Set
        assert parent_children_expected == service.getChildren(parent)
        assert parent_children_expected == service.getChildren(parent)

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()

        // Test New Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(newChild)
        assert child_parent_expected == service.getParents(newChild)
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()

        // rename back
        when:
        service.rename(newChild, child)
        parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert parent_children_expected == service.getChildren(parent)
        assert parent_children_expected == service.getChildren(parent)

        // Test New Child
        assert service.getParents(newChild).isEmpty()
        assert service.getParents(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()

        // Test Child
        assert child_parent_expected == service.getParents(child)
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()
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
        service.drop(child)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert service.getChildren(child).isEmpty()
    }
}
