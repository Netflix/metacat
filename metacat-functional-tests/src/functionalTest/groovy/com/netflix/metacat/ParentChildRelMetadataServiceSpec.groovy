package com.netflix.metacat

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.model.ChildInfo
import com.netflix.metacat.common.server.model.ParentInfo
import com.netflix.metacat.common.server.properties.ParentChildRelationshipProperties
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService
import com.netflix.metacat.common.server.usermetadata.ParentChildRelServiceException
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
    private ParentChildRelationshipProperties props = new ParentChildRelationshipProperties(null);

    @Shared
    static final String SQL_CREATE_PARENT_CHILD_RELATIONS =
        "INSERT INTO parent_child_relation (parent, parent_uuid, child, child_uuid, relation_type) " +
            "VALUES (?, ?, ?, ?, ?)"

    private void insertNewParentChildRecord(final String pName, final String pUUID,
                                            final String child, final String childUUID, final String type) {
        jdbcTemplate.update(SQL_CREATE_PARENT_CHILD_RELATIONS, pName, pUUID, child, childUUID, type)
    }

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
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent) == parent_children_expected
        assert !service.isChildTable(parent)
        assert service.isParentTable(parent)

        // Test Child
        assert service.getParents(child) == child_parent_expected
        assert service.getParents(child) == child_parent_expected
        assert service.isChildTable(child)
        assert !service.isParentTable(child)

        when:
        service.deleteParentChildRelation(parent, parentUUID, child, childUUID, type)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getParents(child).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

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
        service.createParentChildRelation(parent, parentUUID, child1, child1UUID, type, props)
        service.createParentChildRelation(parent, parentUUID, child2, child2UUID, type, props)

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert parent_children_expected == service.getChildren(parent)
        assert !service.isChildTable(parent)
        assert service.isParentTable(parent)

        // Test Children
        assert child_parent_expected == service.getParents(child1)
        assert service.getChildren(child1).isEmpty()
        assert service.isChildTable(child1)
        assert !service.isParentTable(child1)

        assert child_parent_expected == service.getParents(child2)
        assert service.getChildren(child2).isEmpty()
        assert service.isChildTable(child2)
        assert !service.isParentTable(child2)
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
        service.createParentChildRelation(parent1, parent1UUID, child, childUUID, type, props)

        when:
        service.createParentChildRelation(parent2, parent2UUID, child, childUUID, type, props)

        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("Cannot have a child table having more than one parent")

        // Test Child
        def child_parent_expected = [new ParentInfo(parent1.toString(), type, parent1UUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(child)
        assert !service.isParentTable(child)
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
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

        when:
        service.createParentChildRelation(child, childUUID, grandChild, grandChildUUID, type, props)

        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("Cannot create a child table as parent")

        // Test Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert service.getParents(child) == child_parent_expected
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(child)
        assert !service.isParentTable(child)
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
        service.createParentChildRelation(child, childUUID, grandChild, grandChildUUID, type, props)

        when:
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

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
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

        when:
        def pair = service.renameSoftInsert(parent, newParent)
        service.drop(parent, Optional.of(pair))

        then:
        assert pair.getLeft().size() == 1
        assert pair.getRight().isEmpty()

        // Test Old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

        // Test New Parent Name
        assert service.getParents(newParent).isEmpty()
        def newParent_children_expected = [new ChildInfo(child.toString(), type, childUUID)] as Set
        assert service.getChildren(newParent) == newParent_children_expected
        assert !service.isChildTable(newParent)
        assert service.isParentTable(newParent)

        // Test Child
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(child)
        assert !service.isParentTable(child)

        // rename back
        when:
        pair = service.renameSoftInsert(newParent, parent)
        service.drop(newParent, Optional.of(pair))
        child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set

        then:
        assert pair.getLeft().size() == 1
        assert pair.getRight().isEmpty()
        // Test new Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()
        assert !service.isChildTable(newParent)
        assert !service.isParentTable(newParent)

        // Test old Parent Name
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent) == newParent_children_expected
        assert !service.isChildTable(parent)
        assert service.isParentTable(parent)

        // Test Child
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(child)
        assert !service.isParentTable(child)
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
        service.createParentChildRelation(parent, parentUUID, child1, child1UUID, type, props)
        service.createParentChildRelation(parent, parentUUID, child2, child2UUID, type, props)
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        def child_parent_expected = [new ParentInfo(newParent.toString(), type, parentUUID)] as Set
        def newParent_children_expected = [
            new ChildInfo(child1.toString(), type, child1UUID),
            new ChildInfo(child2.toString(), type, child2UUID)
        ] as Set

        when:
        def pair = service.renameSoftInsert(parent, newParent)
        service.drop(parent, Optional.of(pair))

        then:
        // Test Old Parent Name
        assert pair.getLeft().size() == 1
        assert pair.getRight().isEmpty()
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

        // Test New Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent) == newParent_children_expected
        assert !service.isChildTable(newParent)
        assert service.isParentTable(newParent)
        // Test Child1
        assert service.getParents(child1) == child_parent_expected
        assert service.isChildTable(child1)
        assert !service.isParentTable(child1)
        //Test Child2
        assert service.getParents(child2) == child_parent_expected
        assert service.isChildTable(child1)
        assert !service.isParentTable(child1)
    }

    def "Test Rename Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        def pair = service.renameSoftInsert(child, newChild)
        service.drop(child, Optional.of(pair))

        then:
        assert pair.getLeft().isEmpty()
        assert pair.getRight().size() == 1
        // Test Parent
        assert service.getParents(parent).isEmpty()
        def parent_children_expected  = [new ChildInfo(newChild.toString(), type, childUUID)] as Set
        assert parent_children_expected == service.getChildren(parent)
        assert !service.isChildTable(parent)
        assert service.isParentTable(parent)

        // Test old Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert !service.isChildTable(child)
        assert !service.isParentTable(child)

        // Test New Child
        def child_parent_expected = [new ParentInfo(parent.toString(), type, parentUUID)] as Set
        assert child_parent_expected == service.getParents(newChild)
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(newChild)
        assert !service.isParentTable(newChild)

        // rename back
        when:
        pair = service.renameSoftInsert(newChild, child)
        service.drop(newChild, Optional.of(pair))
        parent_children_expected  = [new ChildInfo(child.toString(), type, childUUID)] as Set

        then:
        assert pair.getLeft().isEmpty()
        assert pair.getRight().size() == 1
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert parent_children_expected == service.getChildren(parent)
        assert !service.isChildTable(parent)
        assert service.isParentTable(parent)

        // Test New Child
        assert service.getParents(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()
        assert !service.isChildTable(newChild)
        assert !service.isParentTable(newChild)

        // Test Child
        assert child_parent_expected == service.getParents(child)
        assert service.getChildren(child).isEmpty()
        assert service.isChildTable(child)
        assert !service.isParentTable(child)
    }


    def "Test Drop Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone";
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        when:
        service.drop(child, Optional.empty())

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert !service.isChildTable(child)
        assert !service.isParentTable(child)
    }

    def "Test Rename and Drop Child"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def newChild = QualifiedName.ofTable(catalog, database, "nc")
        def childUUID = "c_uuid"
        def type = "clone";
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

        when:
        def pair = service.renameSoftInsert(child, newChild)
        service.drop(child, Optional.of(pair))
        service.drop(newChild, Optional.empty())

        then:
        // Test Parent
        assert service.getParents(parent).isEmpty()
        assert service.getChildren(parent).isEmpty()
        assert !service.isChildTable(parent)
        assert !service.isParentTable(parent)

        // Test Child
        assert service.getParents(child).isEmpty()
        assert service.getChildren(child).isEmpty()
        assert !service.isChildTable(child)
        assert !service.isParentTable(child)

        // Test newChild
        assert service.getParents(newChild).isEmpty()
        assert service.getChildren(newChild).isEmpty()
        assert !service.isChildTable(newChild)
        assert !service.isParentTable(newChild)
    }

    def "rename to an existing tableName in parent child relationship service"() {
        setup:
        def parent1 = QualifiedName.ofTable(catalog, database, "p1")
        def parent1UUID = "p_uuid1"
        def parent2 = QualifiedName.ofTable(catalog, database, "p2")
        def parent2UUID = "p_uuid2"
        def child1 = QualifiedName.ofTable(catalog, database, "c1")
        def child1UUID = "c1_uuid"
        def child2 = QualifiedName.ofTable(catalog, database, "c2")
        def child2UUID = "c2_uuid"
        def type = "clone";
        service.createParentChildRelation(parent1, parent1UUID, child1, child1UUID, type, props)
        service.createParentChildRelation(parent2, parent2UUID, child2, child2UUID, type, props)
        def child1Parent = [new ParentInfo(parent1.toString(), type, parent1UUID)] as Set
        def parent1Children = [new ChildInfo(child1.toString(), type, child1UUID)] as Set
        def child2Parent = [new ParentInfo(parent2.toString(), type, parent2UUID)] as Set
        def parent2Children = [new ChildInfo(child2.toString(), type, child2UUID)] as Set

        // rename to an existing parent
        when:
        service.renameSoftInsert(parent1, parent2)
        then:
        def e = thrown(Exception)
        assert e.message.contains("is already a parent table")

        // rename to an existing child
        when:
        service.renameSoftInsert(child2, child1)
        then:
        e = thrown(Exception)
        assert e.message.contains("is already a child table")

        //Test p1
        assert service.getParents(parent1).isEmpty()
        assert service.getChildren(parent1) == parent1Children
        assert !service.isChildTable(parent1)
        assert service.isParentTable(parent1)

        //Test c1
        assert service.getParents(child1) == child1Parent
        assert service.getChildren(child1).isEmpty()
        assert service.isChildTable(child1)
        assert !service.isParentTable(child1)

        //Test p2
        assert service.getParents(parent2).isEmpty()
        assert service.getChildren(parent2) == parent2Children
        assert !service.isChildTable(parent2)
        assert service.isParentTable(parent2)

        //Test c2
        assert service.getParents(child2) == child2Parent
        assert service.getChildren(child2).isEmpty()
        assert service.isChildTable(child2)
        assert !service.isParentTable(child2)
    }

    // This could happen in 2 cases:
    // 1. Fail to create the table but did not clean up the parent child relationship
    // 2: Successfully Drop the table but fail to clean up the parent child relationship
    def "simulate a record is not cleaned up and a same parent or child name is created"() {
        given:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "parent_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "child_uuid"
        def type = "clone"
        def randomParent = QualifiedName.ofTable(catalog, database, "rp")
        def randomParentUUID = "random_parent_uuid"
        def randomChild = QualifiedName.ofTable(catalog, database, "rc")
        def randomChildUUID = "random_child_uuid"

        insertNewParentChildRecord(parent.toString(), parentUUID, child.toString(), childUUID, type)

        // Same parent name is created with a different uuid should fail
        when:
        service.createParentChildRelation(parent, randomParentUUID, randomChild, randomChildUUID, type, props)
        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("This normally means table prodhive/testpc/p already exists")

        // Same childName with a different uuid should fail
        when:
        service.createParentChildRelation(randomParent, randomParentUUID, child, randomChildUUID, type, props)
        then:
        e = thrown(RuntimeException)
        assert e.message.contains("Cannot have a child table having more than one parent")
    }

    def "Test maxCloneAllow"() {
        given:
        def catalog = 'testhive'
        def targetParentDB = 'test'
        def targetParentTable = "parent"
        def targetType = "CLONE"
        def parentQName = QualifiedName.ofTable(catalog, targetParentDB, targetParentTable)

        def targetChildDB = 'testChild'
        def targetChildPrefix = 'testChild'

        def parentChildProps = new ParentChildRelationshipProperties(null)
        parentChildProps.setMaxAllow(maxAllow)
        parentChildProps.setDefaultMaxAllowPerRelType(defaultMaxAllowPerRelStr)
        parentChildProps.setMaxAllowPerDBPerRelType(maxAllowPerDBPerRelTypeStr)
        parentChildProps.setMaxAllowPerTablePerRelType(maxAllowPerTablePerRelTypeStr)

        // create expected amount of child should all succeed
        when:
        def String child_name = ""
        def childQualifiedName = null
        for (int i = 0; i < expectedChildAllowCount; i++) {
            child_name = targetChildPrefix + i
            childQualifiedName = QualifiedName.ofTable(catalog, targetChildDB, child_name)
            service.createParentChildRelation(parentQName, targetParentTable, childQualifiedName, child_name, targetType, parentChildProps)
        }
        then:
        noExceptionThrown()
        service.getChildren(parentQName).size() == expectedChildAllowCount

        // create one more with the same type should fail
        when:
        child_name = targetChildPrefix + expectedChildAllowCount
        childQualifiedName = QualifiedName.ofTable(catalog, targetChildDB, child_name)
        service.createParentChildRelation(parentQName, targetParentTable, childQualifiedName, child_name, targetType, parentChildProps)
        then:
        def e = thrown(ParentChildRelServiceException)
        assert e.message.contains("is not allow to have more than $expectedChildAllowCount child table")
        service.getChildren(parentQName).size() == expectedChildAllowCount

        // create one more with different type should succeed
        when:
        service.createParentChildRelation(parentQName, targetParentTable, childQualifiedName, child_name, "random", parentChildProps)
        then:
        noExceptionThrown()
        service.getChildren(parentQName).size() == (expectedChildAllowCount + 1)

        //change the config to -1, and now it should allow new creation
        when:
        if (!maxAllowPerTablePerRelTypeStr.isEmpty() && maxAllowPerTablePerRelTypeStr.contains("CLONE,testhive/test/parent")) {
            def p = /CLONE,testhive\/test\/parent,\d+/
            maxAllowPerTablePerRelTypeStr = maxAllowPerTablePerRelTypeStr.replaceAll(p, "CLONE,testhive/test/parent,-1")
            parentChildProps.setMaxAllowPerTablePerRelType(maxAllowPerTablePerRelTypeStr)
        } else if (!maxAllowPerDBPerRelTypeStr.isEmpty() && maxAllowPerDBPerRelTypeStr.contains("CLONE,test")) {
            def pattern = /CLONE,test,\d+/
            maxAllowPerDBPerRelTypeStr = maxAllowPerDBPerRelTypeStr.replaceAll(pattern, "CLONE,test,-1")
            parentChildProps.setMaxAllowPerDBPerRelType(maxAllowPerDBPerRelTypeStr)
        } else if (!defaultMaxAllowPerRelStr.isEmpty() && defaultMaxAllowPerRelStr.contains("CLONE")) {
            def pattern = /CLONE,\d+/
            defaultMaxAllowPerRelStr = defaultMaxAllowPerRelStr.replaceAll(pattern, "CLONE,-1")
            parentChildProps.setDefaultMaxAllowPerRelType(defaultMaxAllowPerRelStr)
        } else {
            parentChildProps.setMaxAllow(-1)
        }
        child_name = targetChildPrefix + (expectedChildAllowCount + 1)
        childQualifiedName = QualifiedName.ofTable(catalog, targetChildDB, child_name)
        service.createParentChildRelation(parentQName, targetParentTable, childQualifiedName, child_name, targetType, parentChildProps)

        then:
        noExceptionThrown()
        service.getChildren(parentQName).size() == (expectedChildAllowCount + 2)
        assert (parentChildProps.getMaxAllow() == -1 ? 1 : 0) +
            (defaultMaxAllowPerRelStr.contains("-1") ? 1 : 0) +
            (maxAllowPerDBPerRelTypeStr.contains("-1") ? 1 : 0) +
            (maxAllowPerTablePerRelTypeStr.contains("-1") ? 1 : 0) == 1

        where:
        maxAllow | defaultMaxAllowPerRelStr | maxAllowPerDBPerRelTypeStr  | maxAllowPerTablePerRelTypeStr                                 | expectedChildAllowCount
        3        |  ""                      | ""                          |  ""                                                           | 3
        3        |  "Other,5"               | "Other,other,2"             |  "Other,testhive/test/other,2"                                | 3
        1        |  "CLONE,5"               | ""                          |  ""                                                           | 5
        1        |  "CLONE,5;Other,3"       | ""                          |  ""                                                           | 5
        1        |  "CLONE,5;Other,3"       | "Other,other,2"             |  "Other,testhive/test/other,2"                                | 5
        1        |  "CLONE,5"               | "CLONE,test,3"              |  ""                                                           | 3
        1        |  "CLONE,5;Other,3"       | "CLONE,test,3;CLONE,other,2"|  ""                                                           | 3
        1        |  "CLONE,5;Other,3"       | "CLONE,test,3;OTHER,other,2"|  "CLONE,testhive/test/other,2"                                | 3
        1        |  "CLONE,5"               | "CLONE,test,3;OTHER,other,2"|  "CLONE,testhive/test/parent,2"                               | 2
        1        |  "CLONE,5;Other,3"       | "CLONE,test,3;CLONE,other,2"|  "CLONE,testhive/test/parent,2;CLONE,testhive/test/other,2"   | 2
    }
    def "Test Parent Rename failed and remove newName"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone";
        def newParent = QualifiedName.ofTable(catalog, database, "np")
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)

        when:
        def pair = service.renameSoftInsert(parent, newParent)
        service.drop(newParent, Optional.of(pair))

        then:
        // Test New Parent Name
        assert service.getParents(newParent).isEmpty()
        assert service.getChildren(newParent).isEmpty()

        // Test Old Parent Name
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
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        def pair = service.renameSoftInsert(child, newChild)
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
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        def newParentQualifiedName = QualifiedName.ofTable(catalog, database, "np_name")
        def newChildQualifiedName = QualifiedName.ofTable(catalog, database, "nc_name")
        insertNewParentChildRecord(newParentQualifiedName.toString(), "np_uuid", newChildQualifiedName.toString(), "nc_uuid", "random")

        when:
        service.renameSoftInsert(parent, newParentQualifiedName)
        then:
        def e = thrown(RuntimeException)
        assert e.message.contains("is already a parent table")

        when:
        service.renameSoftInsert(child, newChildQualifiedName)

        then:
        e = thrown(RuntimeException)
        assert e.message.contains("is already a child table")
    }

    // Add a test where between rename and drop old/new name with the same name but different uuid is added
    // only those get from rename should be drop
    // This case is not possible in reality but wanted to add a test to prove this
    def "Simulate Rename child: drop only impacted uuids"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")

        when:
        def pair = service.renameSoftInsert(child, newChild)
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

        // Test new child
        assert service.getParents(newChild) == child_parent_expected
        assert service.getChildren(newChild).isEmpty()

        // Test old child
        assert service.getParents(child) == child_parent_expected
        assert service.getChildren(child).isEmpty()
    }

    // Add a test where between rename and drop old/new name same name but different uuid is added
    // only those get from rename should be drop
    // This case is not possible in reality but wanted to add a test to prove this
    def "Simulate Rename Parent: drop only impacted uuids"() {
        setup:
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def parentUUID = "p_uuid"
        def child = QualifiedName.ofTable(catalog, database, "c")
        def childUUID = "c_uuid"
        def type = "clone"
        service.createParentChildRelation(parent, parentUUID, child, childUUID, type, props)
        def newParent = QualifiedName.ofTable(catalog, database, "np")

        when:
        def pair = service.renameSoftInsert(parent, newParent)
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
