
package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.List;


/**
 * Test PolarisConnectorTableService.
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
@Disabled
public class PolarisConnectorDatabaseServiceFunctionalTest extends PolarisConnectorDatabaseServiceTest {
    /**
     * Test SimpleDBList.
     */
    @Test
    public void testSimpleListDb() {
        // Simulate a delay so that the dbs schema is visible
        TestUtil.simulateDelay();
        final DatabaseInfo db1 = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri1").build();
        final DatabaseInfo db2 = DatabaseInfo.builder().name(DB2_QUALIFIED_NAME).uri("uri2").build();
        getPolarisDBService().create(getRequestContext(), db1);
        getPolarisDBService().create(getRequestContext(), db2);
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB1_QUALIFIED_NAME));
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB2_QUALIFIED_NAME));

        // Since now list dbs use follower_read_timestamp, we will not immediately get the newly created dbs
        List<QualifiedName> dbNames =
            getPolarisDBService().listNames(
                getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        List<DatabaseInfo> dbs =
            getPolarisDBService().list(
                getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertTrue("Expected dbNames to be empty", dbNames.isEmpty());
        Assert.assertTrue("Expected dbs to be empty", dbs.isEmpty());


        // After sufficient time, the dbs should return using follower_read_timestamp
        TestUtil.simulateDelay();
        dbNames = getPolarisDBService().listNames(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME, DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        // Test Prefix
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), QualifiedName.ofDatabase(CATALOG_NAME, "db"),
            null,
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME, DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            QualifiedName.ofDatabase(CATALOG_NAME, "db"),
            null,
            null);
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            QualifiedName.ofDatabase(CATALOG_NAME, "db1_"),
            null,
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            QualifiedName.ofDatabase(CATALOG_NAME, "db1_"),
            null,
            null);
        Assert.assertEquals(dbs, Arrays.asList(db1));

        // Test Order desc
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            null,
            new Sort("name", SortOrder.DESC),
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME, DB1_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            null,
            new Sort("name", SortOrder.DESC),
            null);
        Assert.assertEquals(dbs, Arrays.asList(db2, db1));

        // Test pageable
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            null,
            null,
            new Pageable(5, 0));
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME, DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME),
            null,
            null,
            new Pageable(5, 0));
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(1, 0));
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(1, 0));
        Assert.assertEquals(dbs, Arrays.asList(db1));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(1, 1));
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(1, 1));
        Assert.assertEquals(dbs, Arrays.asList(db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(5, 1));
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME), null, null, new Pageable(5, 1));
        Assert.assertEquals(dbs, Arrays.asList(db2));
    }
}

