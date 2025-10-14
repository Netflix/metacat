
package com.netflix.metacat.connector.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceReaderConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisStoreConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.spectator.api.NoopRegistry;
import lombok.Getter;
import org.apache.iceberg.shaded.org.apache.orc.storage.common.util.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {
    PolarisStoreConfig.class,
    PolarisPersistenceConfig.class,
    PolarisPersistenceReaderConfig.class
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
@Getter
public class PolarisConnectorDatabaseServiceFunctionalTest {
    /**
     * The name of the catalog used for the databases.
     */
    public static final String CATALOG_NAME_TEST = "testdb";

    /**
     * The name of the catalog used for the databases.
     */
    public static final String CATALOG_NAME_PROD = "proddb";

    /**
     * The name of the first database.
     */
    public static final String DB1_NAME = "db1_name";

    /**
     * The name of the second database.
     */
    public static final String DB2_NAME = "db2_name";

    /**
     * The qualified name of the first database, which includes the test catalog name and the first database name.
     */
    public static final QualifiedName DB1_QUALIFIED_NAME_TEST_CATALOG =
        QualifiedName.ofDatabase(CATALOG_NAME_TEST, DB1_NAME);

    /**
     * The qualified name of the first database, which includes the prod catalog name and the first database name.
     */
    public static final QualifiedName DB1_QUALIFIED_NAME_PROD_CATALOG =
        QualifiedName.ofDatabase(CATALOG_NAME_PROD, DB1_NAME);

    /**
     * The qualified name of the second database, which includes the catalog name and the second database name.
     */
    public static final QualifiedName DB2_QUALIFIED_NAME_TEST_CATALOG =
        QualifiedName.ofDatabase(CATALOG_NAME_TEST, DB2_NAME);

    @Autowired
    private Environment environment;

    @Autowired
    private PolarisStoreService polarisStoreService;

    private ConnectorContext connectorContext;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    /**
     * Initialization.
     */
    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    @BeforeEach
    public void init() {
        final String[] activeProfiles = environment.getActiveProfiles();
        assert activeProfiles.length  == 1;

        connectorContext = new ConnectorContext(
            CATALOG_NAME_TEST,
            CATALOG_NAME_TEST, "polaris",
            new DefaultConfigImpl(
                new MetacatProperties(null,
                    environment.getActiveProfiles()[0].equals("polaris_functional_aurora_test"))),
            new NoopRegistry(),
            null,
            Maps.newHashMap()
        );
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService, connectorContext);
    }

    /**
     * Test create database.
     */
    @Test
    public void testCreateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test get database that exists.
     */
    @Test
    public void testGetDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri("uri").build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info, result);
        Assert.assertTrue(result.getMetadata().isEmpty());
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test get database not found.
     */
    @Test
    public void testGetDbNotFound() {
        Assertions.assertThrows(DatabaseNotFoundException.class,
            () -> polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
    }

    /**
     * Test create database that already exists.
     */
    @Test
    public void testCreateDbAlreadyExists() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        Assertions.assertThrows(DatabaseAlreadyExistsException.class,
            () -> polarisDBService.create(requestContext, info));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test create database with no uri set should fallback to default uri.
     */
    @Test
    public void testCreateDbDefaultUri() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri("db1_name.db").build();
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(infoExpected, result);
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test create database get audit info.
     */
    @Test
    public void testDbAudit() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).build();
        final Date date = new Date();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri(DB1_NAME + ".db").build();
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(infoExpected, result);
        final AuditInfo auditInfo = result.getAudit();
        Assert.assertNotNull(auditInfo);
        Assert.assertTrue(auditInfo.getCreatedDate().after(date));
        Assert.assertEquals(auditInfo.getCreatedDate(), auditInfo.getLastModifiedDate());
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test update database.
     */
    @Test
    public void testUpdateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri("uri").build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        polarisDBService.update(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info, result);
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test create database and update params/metadata.
     */
    @Test
    public void testDbParams() {
        DatabaseInfo info = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG)
            .metadata(ImmutableMap.of("owner", "bdp"))
            .build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri(DB1_NAME + ".db")
            .metadata(ImmutableMap.of("owner", "bdp"))
            .build();
        info = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(infoExpected, info);
        Assert.assertEquals(info.getMetadata().get("owner"), "bdp");

        info.setMetadata(null);
        polarisDBService.update(requestContext, info);
        info = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info.getMetadata().get("owner"), "bdp");

        info.setMetadata(ImmutableMap.of("owner", "bdp_updated"));
        polarisDBService.update(requestContext, info);
        info = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info.getMetadata().get("owner"), "bdp_updated");

        info.setMetadata(ImmutableMap.of("owner2", "bdp2"));
        polarisDBService.update(requestContext, info);
        info = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info.getMetadata().get("owner"), "bdp_updated");
        Assert.assertEquals(info.getMetadata().get("owner2"), "bdp2");

        info.setMetadata(new HashMap<>());
        polarisDBService.update(requestContext, info);
        info = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertEquals(info.getMetadata().get("owner"), "bdp_updated");
        Assert.assertEquals(info.getMetadata().get("owner2"), "bdp2");

        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
    }

    /**
     * Test delete database.
     */
    @Test
    public void testDeleteDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assert.assertFalse(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
    }


    /**
     * Test SimpleDBList.
     */
    @Test
    public void testSimpleListDb() {
        // Simulate a delay so that the dbs schema is visible
        TestUtil.simulateDelay();
        final DatabaseInfo db1 = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri("uri1").build();
        final DatabaseInfo db2 = DatabaseInfo.builder().name(DB2_QUALIFIED_NAME_TEST_CATALOG).uri("uri2").build();
        getPolarisDBService().create(getRequestContext(), db1);
        getPolarisDBService().create(getRequestContext(), db2);
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB1_QUALIFIED_NAME_TEST_CATALOG));
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB2_QUALIFIED_NAME_TEST_CATALOG));

        List<QualifiedName> dbNames = new ArrayList<>();
        List<DatabaseInfo> dbs = new ArrayList<>();

        // Since crdb uses follower_read_timestamp, we will not immediately get the newly created dbs
        if (environment.getActiveProfiles()[0].equals("polaris_functional_test")) {
            dbNames =
                getPolarisDBService().listNames(
                    getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
            dbs =
                getPolarisDBService().list(
                    getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
            Assert.assertTrue("Expected dbNames to be empty", dbNames.isEmpty());
            Assert.assertTrue("Expected dbs to be empty", dbs.isEmpty());
        }


        // After sufficient time, the dbs should return using follower_read_timestamp
        TestUtil.simulateDelay();
        dbNames = getPolarisDBService().listNames(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG, DB2_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        // Test Prefix
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), QualifiedName.ofDatabase(CATALOG_NAME_TEST, "db"),
            null,
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG, DB2_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            QualifiedName.ofDatabase(CATALOG_NAME_TEST, "db"),
            null,
            null);
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            QualifiedName.ofDatabase(CATALOG_NAME_TEST, "db1_"),
            null,
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            QualifiedName.ofDatabase(CATALOG_NAME_TEST, "db1_"),
            null,
            null);
        Assert.assertEquals(dbs, Arrays.asList(db1));

        // Test Order desc
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            null,
            new Sort("name", SortOrder.DESC),
            null);
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME_TEST_CATALOG, DB1_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            null,
            new Sort("name", SortOrder.DESC),
            null);
        Assert.assertEquals(dbs, Arrays.asList(db2, db1));

        // Test pageable
        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            null,
            null,
            new Pageable(5, 0));
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG, DB2_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            null,
            null,
            new Pageable(5, 0));
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(1, 0));
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(1, 0));
        Assert.assertEquals(dbs, Arrays.asList(db1));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(1, 1));
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(1, 1));
        Assert.assertEquals(dbs, Arrays.asList(db2));

        dbNames = getPolarisDBService().listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(5, 1));
        Assert.assertEquals(dbNames, Arrays.asList(DB2_QUALIFIED_NAME_TEST_CATALOG));
        dbs = getPolarisDBService().list(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, new Pageable(5, 1));
        Assert.assertEquals(dbs, Arrays.asList(db2));

        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        polarisDBService.delete(requestContext, DB2_QUALIFIED_NAME_TEST_CATALOG);
    }


    /**
     * Test list database with different db page size config.
     */
    @Test
    public void testDifferentCatalogsForDBs() {
        /*
        Create the same db under same catalog
         */
        final DatabaseInfo infoTest = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG).uri("db1_name.db").build();
        polarisDBService.create(requestContext, infoTest);
        Assertions.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));

        final DatabaseInfo infoProd = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME_PROD_CATALOG).uri("db1_name.db")
            .build();
        polarisDBService.create(requestContext, infoProd);
        Assertions.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG));

        /*
        Get the same db under different catalogs
         */
        Assertions.assertEquals(infoTest, polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        Assertions.assertEquals(infoProd, polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG));

        /*
        Update the same db under different catalogs
         */
        requestContext.setUserName("newUser");
        final DatabaseInfo updateInfoTest = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_TEST_CATALOG)
            .uri("db1_name.db")
            .build();
        polarisDBService.update(requestContext, updateInfoTest);
        final DatabaseInfo updateInfoProd = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME_PROD_CATALOG)
            .uri("db1_name.db")
            .build();
        polarisDBService.update(requestContext, updateInfoProd);
        Assertions.assertEquals(updateInfoTest, polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        Assertions.assertEquals(updateInfoProd, polarisDBService.get(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG));

        TestUtil.simulateDelay();

        /*
        Test List Names
         */
        final List<QualifiedName> dbNamesFromTest = polarisDBService.listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_TEST),
            null,
            null,
            null
        );
        Assertions.assertEquals(dbNamesFromTest, Arrays.asList(DB1_QUALIFIED_NAME_TEST_CATALOG));

        final List<QualifiedName> dbNamesFromProd = polarisDBService.listNames(
            getRequestContext(),
            QualifiedName.ofCatalog(CATALOG_NAME_PROD),
            null,
            null,
            null
        );
        Assertions.assertEquals(dbNamesFromProd, Arrays.asList(DB1_QUALIFIED_NAME_PROD_CATALOG));

        /*
        Test listTableEntities
         */
        final List<DatabaseInfo> dbsFromTest = getPolarisDBService().list(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
        Assert.assertEquals(dbsFromTest, Arrays.asList(updateInfoTest));

        final List<DatabaseInfo> dbsFromProd = getPolarisDBService().list(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME_TEST), null, null, null);
        Assert.assertEquals(dbsFromProd, Arrays.asList(updateInfoTest));

        /*
        Delete the same db under different catalogs
         */
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG);
        Assertions.assertFalse(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_TEST_CATALOG));
        Assertions.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG);
        Assertions.assertFalse(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME_PROD_CATALOG));
    }
}

