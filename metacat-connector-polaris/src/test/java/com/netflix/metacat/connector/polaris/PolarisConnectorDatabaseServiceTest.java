
package com.netflix.metacat.connector.polaris;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.spectator.api.NoopRegistry;
import lombok.Getter;
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

import java.util.Date;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polarisconnectortest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
@Getter
public class PolarisConnectorDatabaseServiceTest {
    public static final String CATALOG_NAME = "catalog_name";
    public static final String DB1_NAME = "db1_name";
    public static final String DB2_NAME = "db2_name";
    public static final QualifiedName DB1_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB1_NAME);
    public static final QualifiedName DB2_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB2_NAME);

    @Autowired
    private PolarisStoreService polarisStoreService;

    @Shared
    private ConnectorContext connectorContext;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    /**
     * Initialization.
     */
    @BeforeEach
    public void init() {
        connectorContext = new ConnectorContext(CATALOG_NAME, CATALOG_NAME, "polaris",
            new DefaultConfigImpl(new MetacatProperties(null)), new NoopRegistry(), null,  Maps.newHashMap());
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService, connectorContext);
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
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri").build();
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
     * Test create database with no uri set should fallback to default uri.
     */
    @Test
    public void testCreateDbDefaultUri() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME).uri("db1_name.db").build();
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(infoExpected, result);
    }

    /**
     * Test create database get audit info.
     */
    @Test
    public void testDbAudit() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        Date date = new Date();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME).uri(DB1_NAME + ".db").build();
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(infoExpected, result);
        final AuditInfo auditInfo = result.getAudit();
        Assert.assertNotNull(auditInfo);
        Assert.assertTrue(auditInfo.getCreatedDate().after(date));
        Assert.assertEquals(auditInfo.getCreatedDate(), auditInfo.getLastModifiedDate());
    }

    /**
     * Test update database.
     */
    @Test
    public void testUpdateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri").build();
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
}

