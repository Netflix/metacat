
package com.netflix.metacat.connector.polaris;

import com.google.common.collect.Sets;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.List;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polarisconnectortest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
public class PolarisConnectorDatabaseServiceTest {
    private static final String CATALOG_NAME = "catalog_name";
    private static final String DB1_NAME = "db1_name";
    private static final String DB2_NAME = "db2_name";
    private static final QualifiedName DB1_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB1_NAME);
    private static final QualifiedName DB2_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB2_NAME);

    @Autowired
    private PolarisStoreService polarisStoreService;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    /**
     * Initialization.
     */
    @BeforeEach
    public void init() {
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService);
    }

    /**
     * Test create database.
     */
    @Test
    public void testCreateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
    }

    /**
     * Test get database that exists.
     */
    @Test
    public void testGetDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(info, result);
    }

    /**
     * Test get database not found.
     */
    @Test
    public void testGetDbNotFound() {
        Assertions.assertThrows(DatabaseNotFoundException.class,
            () -> polarisDBService.get(requestContext, DB1_QUALIFIED_NAME));
    }

    /**
     * Test create database that already exists.
     */
    @Test
    public void testCreateDbAlreadyExists() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        Assertions.assertThrows(DatabaseAlreadyExistsException.class,
            () -> polarisDBService.create(requestContext, info));
    }

    /**
     * Test update database.
     */
    @Test
    public void testUpdateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        polarisDBService.update(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(info, result);
    }

    /**
     * Test delete database.
     */
    @Test
    public void testDeleteDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertFalse(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
    }

    /**
     * Test list databases.
     */
    @Test
    public void testListDb() {
        final DatabaseInfo db1 = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        final DatabaseInfo db2 = DatabaseInfo.builder().name(DB2_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, db1);
        polarisDBService.create(requestContext, db2);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        Assert.assertTrue(polarisDBService.exists(requestContext, DB2_QUALIFIED_NAME));
        final List<QualifiedName> dbNames =
            polarisDBService.listNames(requestContext, QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(Sets.newHashSet(dbNames), Sets.newHashSet(DB1_QUALIFIED_NAME, DB2_QUALIFIED_NAME));
        final List<DatabaseInfo> dbs =
            polarisDBService.list(requestContext, QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(Sets.newHashSet(dbs), Sets.newHashSet(db1, db2));
    }
}

