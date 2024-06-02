package com.netflix.metacat

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.model.ChildInfo
import com.netflix.metacat.common.server.model.ParentInfo
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService
import com.netflix.metacat.metadata.mysql.MySqlParentChildRelMetaDataService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.retry.backoff.NoBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import spock.lang.Shared
import spock.lang.Specification
import static org.junit.jupiter.api.Assertions.assertThrows

class ParentChildRelMetadataServiceSpec extends Specification{
    @Shared
    private ParentChildRelMetadataService service;
    @Shared
    private JdbcTemplate jdbcTemplate;
    @Shared
    private RetryTemplate retryTemplate;

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

        RetryTemplate retryTemplate = new RetryTemplate()

        // Configure no backoff policy
        retryTemplate.setBackOffPolicy(new NoBackOffPolicy())

        // Configure retry policy with no retries
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(1) // 1 attempt means no retries
        retryTemplate.setRetryPolicy(retryPolicy)

        return retryTemplate

        jdbcTemplate = new JdbcTemplate(dataSource)
        service = new MySqlParentChildRelMetaDataService(jdbcTemplate, retryTemplate)
    }

    def cleanup() {
        jdbcTemplate.update("DELETE FROM parent_child_relation")
    }

    def "Test Create - OneChildOneParent"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone";
        service.createParentChildRelation(parent, child, type)

        // Test Parent
        assert service.getParent(parent) == null

        def parent_children_expected  = [new ChildInfo(child.toString(), type)] as Set
        assert service.getChildren(parent) == parent_children_expected

        // Test Child
        def child_parent_expected = new ParentInfo(parent.toString(), type)
        assert service.getParent(child) == child_parent_expected

        assert service.getChildren(child).size() == 0
    }

    def "Test Create - oneParentMultiChildren"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child1 = QualifiedName.ofTable(catalog, database, "c1")
        def child2 = QualifiedName.ofTable(catalog, database, "c2")
        def type = "clone";
        service.createParentChildRelation(parent, child1, type)
        service.createParentChildRelation(parent, child2, type)

        // Test Parent
        assert service.getParent(parent) == null

        def parent_children_expected  = [
            new ChildInfo(child1.toString(), type),
            new ChildInfo(child2.toString(), type),
        ] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test Children
        def child_parent_expected = new ParentInfo(parent.toString(), type)

        // Test Child 1
        assert child_parent_expected == service.getParent(child1)
        assert service.getChildren(child1) == 0

        assert child_parent_expected == service.getParent(child2)
        assert service.getChildren(child2).size() == 0
    }

    def "Test Create - oneChildMultiParentException"() {
        setup:
        QualifiedName.ofTable()
        def parent1 = QualifiedName.ofTable(catalog, database, "p1")
        def parent2 = QualifiedName.ofTable(catalog, database, "p2")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone"
        service.createParentChildRelation(parent1, child, type)
        assertThrows(RuntimeException.class, service.createParentChildRelation(parent2, child, type))

        // Test Child
        def child_parent_expected = new ParentInfo(parent1.toString(), type)
        assert child_parent_expected == service.getParent(child)

        assert service.getChildren(child).size() == 0
    }

    def "Test Create - oneChildAsParentOfAnotherException"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def grandChild = QualifiedName.ofTable(catalog, database, "gc")
        def type = "clone"
        service.createParentChildRelation(parent, child, type)
        assertThrows(RuntimeException.class, service.createParentChildRelation(child, grandChild, type))

        // Test Child
        def child_parent_expected = new ParentInfo(parent.toString(), type)
        def child_parent_actual = service.getParent(child)
        assert child_parent_expected == child_parent_actual

        def child_children_actual = service.getChildren(child)
        assert child_children_actual.size() == 0
    }

    def "Test Rename Parent - One Child"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone";
        service.createParentChildRelation(parent, child, type)

        def newParent = QualifiedName.ofTable(catalog, database, "np")
        service.rename(parent, newParent)

        // Test Old Parent Name
        assert service.getParent(parent) == null
        assert service.getChildren(parent).size() == 0

        // Test New Parent Name
        assert service.getParent(newParent) == null
        def newParent_children_expected = [new ChildInfo(child.toString(), type)] as Set
        assert service.getChildren(newParent) == newParent_children_expected

        // Test Child
        def child_parent_expected = new ParentInfo(parent.toString(), type)
        assert child_parent_expected == service.getParent(child)
        assert service.getChildren(child).size() == 0
    }

    def "Test Rename Parent - Multi Child"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child1 = QualifiedName.ofTable(catalog, database, "c1")
        def child2 = QualifiedName.ofTable(catalog, database, "c2")
        def type = "clone";
        service.createParentChildRelation(parent, child1, type)
        service.createParentChildRelation(parent, child2, type)

        def newParent = QualifiedName.ofTable(catalog, database, "np")
        service.rename(parent, newParent)

        def child_parent_expected = new ParentInfo(newParent.toString(), type)
        // Test Child1
        assert service.getParent(child1) == child_parent_expected
        //Test Child2
        assert service.getParent(child2) == child_parent_expected
    }

    def "Test Rename Child"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone"
        service.createParentChildRelation(parent, child, type)
        def newChild = QualifiedName.ofTable(catalog, database, "nc")
        service.rename(child, newChild)

        // Test Parent
        assert service.getParent(parent) == null
        def parent_children_expected  = [new ChildInfo(child.toString(), type)] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test Child
        assert null == service.getParent(child)
        assert service.getChildren(child).size() == 0

        // Test New Child
        def child_parent_expected = new ParentInfo(parent.toString(), type)
        assert child_parent_expected == service.getParent(child)
        assert service.getChildren(child).size() == 0
    }


    def "Test Drop Child"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone";
        service.createParentChildRelation(parent, child, type)

        // Test Parent
        def parent_parent_actual = service.getParent(parent)
        assert parent_parent_actual == null

        def parent_children_actual = service.getChildren(parent)
        assert parent_children_actual.size() == 0

        // Test Child
        def child_parent_actual = service.getParent(child)
        assert null == child_parent_actual

        def child_children_actual = service.getChildren(child)
        assert child_children_actual.size() == 0
    }

    def "Test Drop parent - Child still exists Exception"() {
        setup:
        QualifiedName.ofTable()
        def parent = QualifiedName.ofTable(catalog, database, "p")
        def child = QualifiedName.ofTable(catalog, database, "c")
        def type = "clone";
        service.createParentChildRelation(parent, child, type)
        assertThrows(RuntimeException.class, service.drop(parent))

        // Test Parent
        def parent_parent_actual = service.getParent(parent)
        assert parent_parent_actual == null

        def parent_children_expected  = [new ChildInfo(child.toString(), type)] as Set
        assert parent_children_expected == service.getChildren(parent)

        // Test Child
        def child_parent_expected = new ParentInfo(parent.toString(), type)
        assert child_parent_expected == service.getParent(child)

        def child_children_actual = service.getChildren(child)
        assert child_children_actual.size() == 0
    }
}
