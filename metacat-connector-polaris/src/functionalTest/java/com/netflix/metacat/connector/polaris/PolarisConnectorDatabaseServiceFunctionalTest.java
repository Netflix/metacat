package com.netflix.metacat.connector.polaris;

import com.github.javaparser.utils.Log;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisStoreConfig.class, PolarisPersistenceConfig.class, PolarisPersistenceReaderConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
@Getter
public class PolarisConnectorDatabaseServiceFunctionalTest implements ApplicationRunner {

    public static final String CATALOG_NAME = "catalog_name";
    public static final String DB1_NAME = "db1_name";
    public static final String DB2_NAME = "db2_name";
    public static final QualifiedName DB1_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB1_NAME);
    public static final QualifiedName DB2_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB2_NAME);

    @Autowired
    private Environment environment;

    @Autowired
    private PolarisStoreService polarisStoreService;

    @Autowired
    private ApplicationContext applicationContext;

    @Shared
    private ConnectorContext connectorContext;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // Inspect the beans in the application context
        Log.error("gizz");
        String[] beanNames = applicationContext.getBeanDefinitionNames();
        String result = "hey = ";
        for (String beanName : beanNames) {
            result += beanName + " | ";
        }
        Log.error(result);
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    @BeforeEach
    public void init() {
        String[] activeProfiles = environment.getActiveProfiles();
        assert activeProfiles.length == 1;

        connectorContext = new ConnectorContext(
            CATALOG_NAME,
            CATALOG_NAME, "polaris",
            new DefaultConfigImpl(new MetacatProperties(
                null, activeProfiles[0].equals("polaris_functional_aurora_test"))),
            new NoopRegistry(),
            null,
            Maps.newHashMap()
        );
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService, connectorContext);
    }

    @Test
    public void testCreateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
    }

    @Test
    public void testGetDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri").build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(info, result);
    }

    @Test
    public void testGetDbNotFound() {
        Assertions.assertThrows(DatabaseNotFoundException.class,
            () -> polarisDBService.get(requestContext, DB1_QUALIFIED_NAME));
    }

    @Test
    public void testCreateDbAlreadyExists() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        Assertions.assertThrows(DatabaseAlreadyExistsException.class,
            () -> polarisDBService.create(requestContext, info));
    }

    @Test
    public void testCreateDbDefaultUri() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        final DatabaseInfo infoExpected = DatabaseInfo.builder()
            .name(DB1_QUALIFIED_NAME).uri("db1_name.db").build();
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(infoExpected, result);
    }

    @Test
    public void testDbAudit() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        final Date date = new Date();
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

    @Test
    public void testUpdateDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri").build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        polarisDBService.update(requestContext, info);
        final DatabaseInfo result = polarisDBService.get(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertEquals(info, result);
    }

    @Test
    public void testDeleteDb() {
        final DatabaseInfo info = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).build();
        polarisDBService.create(requestContext, info);
        Assert.assertTrue(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
        polarisDBService.delete(requestContext, DB1_QUALIFIED_NAME);
        Assert.assertFalse(polarisDBService.exists(requestContext, DB1_QUALIFIED_NAME));
    }

    @Test
    public void testSimpleListDb() {
        TestUtil.simulateDelay();
        final DatabaseInfo db1 = DatabaseInfo.builder().name(DB1_QUALIFIED_NAME).uri("uri1").build();
        final DatabaseInfo db2 = DatabaseInfo.builder().name(DB2_QUALIFIED_NAME).uri("uri2").build();
        getPolarisDBService().create(getRequestContext(), db1);
        getPolarisDBService().create(getRequestContext(), db2);
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB1_QUALIFIED_NAME));
        Assert.assertTrue(getPolarisDBService().exists(getRequestContext(), DB2_QUALIFIED_NAME));

        List<QualifiedName> dbNames = new ArrayList<>();
        List<DatabaseInfo> dbs = new ArrayList<>();

        if (!connectorContext.getConfig().isAuroraDataSourceEnabled()) {
            dbNames = getPolarisDBService().listNames(
                getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
            dbs = getPolarisDBService().list(
                getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
            Assert.assertTrue("Expected dbNames to be empty", dbNames.isEmpty());
            Assert.assertTrue("Expected dbs to be empty", dbs.isEmpty());
        }

        TestUtil.simulateDelay();
        dbNames = getPolarisDBService().listNames(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(dbNames, Arrays.asList(DB1_QUALIFIED_NAME, DB2_QUALIFIED_NAME));
        dbs = getPolarisDBService().list(
            getRequestContext(), QualifiedName.ofCatalog(CATALOG_NAME), null, null, null);
        Assert.assertEquals(dbs, Arrays.asList(db1, db2));

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
