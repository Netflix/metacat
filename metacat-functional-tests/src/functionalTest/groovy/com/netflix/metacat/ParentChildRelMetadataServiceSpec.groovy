package com.netflix.metacat

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.model.ChildInfo
import com.netflix.metacat.common.server.model.ParentInfo
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService
import com.netflix.metacat.metadata.mysql.MySqlParentChildRelMetaDataService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import spock.lang.Shared
import spock.lang.Specification

class ParentChildRelMetadataServiceSpec extends Specification{
    @Shared
    private ParentChildRelMetadataService service;
    @Shared
    private JdbcTemplate jdbcTemplate;

    @Shared
    private ConverterUtil converterUtil;

    @Shared
    private String catalog = "prodhive"
    @Shared
    private String database = "testpc"

    @Shared
    static final String SQL_CREATE_PARENT_CHILD_RELATIONS =
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
        def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
        def converter = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))
        service = new MySqlParentChildRelMetaDataService(jdbcTemplate, converter)
    }

    private void insertNewParentChildRecord(final String pName, final String pUUID,
                                                   final String child, final String childUUID, final String type) {
        jdbcTemplate.update(SQL_CREATE_PARENT_CHILD_RELATIONS, pName, pUUID, child, childUUID, type)
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

        assert service.getChildren(parent) == parent_children_expected

        // Test Child
        assert service.getParents(child) == child_parent_expected
        assert service.getChildren(child).isEmpty()

        when:
        service.deleteParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
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

        // Test Children
        // Test Child 1
        assert child_parent_expected == service.getParents(child1)
        assert service.getChildren(child1).isEmpty()

        assert child_parent_expected == service.getParents(child2)
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
        def pair = service.rename(parent, newParent)
        service.drop(parent, Optional.of(pair))

        then:
        // Test Old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test New Parent Name
        assert service.getParents(newParent).isEmpty()
        def newParent_children_expected = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert service.getChildren(newParent) == newParent_children_expected

        // Test Child
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()

        // rename back
        when:
        pair = service.rename(newParent, parent)
        service.drop(newParent, Optional.of(pair))
        child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set

        then:
        // Test new Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()

        // Test old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent) == newParent_children_expected

        // Test Child
        assert child_parent_expected == service.getParents(child)
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
        def pair = service.rename(parent, newParent)
        service.drop(parent, Optional.of(pair))

        then:
        // Test Child1
        assert service.getParents(child1) == child_parent_expected
        assert service.getChildren(child1).isEmpty()
        //Test Child2
        assert service.getParents(child2) == child_parent_expected
        assert service.getChildren(child2).isEmpty()
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
        def pair = service.rename(child, newChild)
        assert pair.getRight().size() == 1
        service.drop(child, Optional.of(pair))
        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID)] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()

        // Test New Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(newChild)
        assert service.getChildren(child).isEmpty()

        // rename back
        when:
        pair = service.rename(newChild, child)
        service.drop(newChild, Optional.of(pair))
        parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert parent_children_expected == service.getChildren(parent)

        // Test New Child
        assert service.getParents(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()

        // Test Child
        assert child_parent_expected == service.getParents(child)
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
        service.drop(child, Optional.empty())

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
    }

    def "Test Parent Rename failed and remove newName"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone";
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)

        when:
        def pair = service.rename(parent, newParent)
        service.drop(newParent, Optional.of(pair))

        then:
        // Test Old Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()

        // Test New Parent Name
        assert service.getParents(parent).isEmpty()
        def newParent_children_expected = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert service.getChildren(parent) == newParent_children_expected

        // Test Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
    }

    def "Test Child Rename failed and remove newName"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        def pair = service.rename(child, newChild)
        service.drop(newChild, Optional.of(pair))
        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test New Child
        assert service.getParents(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()

        // Test Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
    }

    def "Test rename to an existing table in the parent child rel service"() {
        given:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        def newParentQualifiedName = QualifiedName.ofTable(catalog, database, "np_name")
        def newChildQualifiedName = QualifiedName.ofTable(catalog, database, "nc_name")
        insertNewParentChildRecord(newParentQualifiedName.toString(), "np_uuid", newChildQualifiedName.toString(), "nc_uuid", "random")

        when:
        service.rename(parent, newParentQualifiedName)
        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("already exists")

        when:
        service.rename(child, newChildQualifiedName)

        then:
        e = thrown(RuntimeException)
        assert e.message.contains("already exists")
    }

    // Add a test where between rename and drop old/new name same name but different uuid is added but only those get
    // from rename should be drop
    def "Simulate Rename child: drop only impacted uuids"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        def pair = service.rename(child, newChild)
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        // Add a record with the same child name but different uuid and since this record is added after rename
        // this record should not be dropped during the service.drop
        insertNewParentChildRecord(parent.toString(), parentUUID, child.toString(), "c_uuid2", type)
        service.drop(child, Optional.of(pair))
        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID),
                                         new ChildInfo(child.toString(), type, "c_uuid2")] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test child
        assert service.getParents(newChild) == child_parent_expected
        assert service.getChildren(newChild).isEmpty()

        assert service.getParents(child) == child_parent_expected
        assert service.getChildren(child).isEmpty()
    }

    // Add a test where between rename and drop old/new name same name but different uuid is added but only those get
    // from rename should be drop
    def "Simulate Rename Parent: drop only impacted uuids"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type)
        def newParent = QualifiedName.ofTable(catalog, database, "np")

        when:
        def pair = service.rename(parent, newParent)
        // Add a record with the same parent name but different uuid and since this record is added after rename
        // this record should not be dropped during the service.drop
        insertNewParentChildRecord(parent.toString(), "p_uuid2", child.toString(), "c_uuid1", "clone")
        service.drop(parent, Optional.of(pair))
        then:

        // Test p
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(child.toString(), "clone", "c_uuid1")] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test np
        assert service.getParents(newParent).isEmpty()
        def new_parent_children_expected  = [new ChildInfo(child.toString(), "clone", childUUID)] as Set
        assert new_parent_children_expected == service.getChildren(newParent)

        // Test child
        assert service.getChildren((child)).isEmpty()
        def child_parent_expected = [
            new ParentInfo(newParent.toString(), "clone", parentUUID),
            new ParentInfo(parent.toString(), "clone", "p_uuid2"),
        ] as Set
        assert service.getParents(child) == child_parent_expected
    }
}
