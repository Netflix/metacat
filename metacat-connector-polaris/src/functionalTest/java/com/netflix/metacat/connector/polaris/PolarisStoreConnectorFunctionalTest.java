package com.netflix.metacat.connector.polaris;


import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnectorTest;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
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
import java.util.stream.Collectors;

/**
 * Test persistence operations on Database objects.
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
@Disabled
public class PolarisStoreConnectorFunctionalTest extends PolarisStoreConnectorTest {

    /**
     * Test to verify that table names fetch works.
     */
    @Test
    public void testPaginatedFetch() {
        final String dbName = generateDatabaseName();
        createDB(dbName);
        List<String> tblNames = getPolarisConnector().getTables(dbName, "", 1000);
        Assert.assertEquals(0, tblNames.size());

        final String tblNameA = "A_" + generateTableName();
        final String tblNameB = "B_" + generateTableName();
        final String tblNameC = "C_" + generateTableName();
        createTable(dbName, tblNameA);
        createTable(dbName, tblNameB);
        createTable(dbName, tblNameC);

        TestUtil.simulateDelay();

        tblNames = getPolarisConnector().getTables(dbName, "", 1000);
        Assert.assertEquals(3, tblNames.size());
        Assert.assertEquals(tblNameA, tblNames.get(0));
        Assert.assertEquals(tblNameB, tblNames.get(1));
        Assert.assertEquals(tblNameC, tblNames.get(2));
    }

    /**
     * Test getTableEntities.
     */
    @Test
    public void testGetTableEntities() {
        // Create the db
        final String dbName = generateDatabaseName();
        createDB(dbName);

        TestUtil.simulateDelay();

        // Test when db is empty
        List<PolarisTableEntity> entities = getPolarisConnector().getTableEntities(dbName, "", 1);
        Assert.assertEquals(0, entities.size());


        // Add some tables
        final String tblNameA = "A_" + generateTableName();
        final String tblNameB = "B_" + generateTableName();
        final String tblNameC = "C_" + generateTableName();
        createTable(dbName, tblNameA);
        createTable(dbName, tblNameB);
        createTable(dbName, tblNameC);

        TestUtil.simulateDelay();

        // Test pagination and sort
        entities = getPolarisConnector().getTableEntities(dbName, "", 1);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 2);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 3);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());

        entities = getPolarisConnector().getTableEntities(dbName, "", 4);
        Assert.assertEquals(3, entities.size());
        Assert.assertEquals(tblNameA, entities.get(0).getTblName());
        Assert.assertEquals(tblNameB, entities.get(1).getTblName());
        Assert.assertEquals(tblNameC, entities.get(2).getTblName());
    }

    /**
     * test list database with different db page size config.
     */
    @Test
    public void testListDbPage() {
        createDB("db1");
        createDB("db2");
        createDB("db3");

        TestUtil.simulateDelay();

        List<String> dbNames = getPolarisConnector().getDatabaseNames("db", null, 1);
        List<PolarisDatabaseEntity> dbs = getPolarisConnector().getDatabases("db", null, 1);
        Assert.assertEquals("Expected dbNames ", Arrays.asList("db1", "db2", "db3"), dbNames);
        Assert.assertEquals("Expected dbs ", Arrays.asList("db1", "db2", "db3"),
            dbs.stream().map(PolarisDatabaseEntity::getDbName).collect(Collectors.toList()));

        dbNames = getPolarisConnector().getDatabaseNames("db", null, 2);
        dbs = getPolarisConnector().getDatabases("db", null, 2);
        Assert.assertEquals("Expected dbNames ", Arrays.asList("db1", "db2", "db3"), dbNames);
        Assert.assertEquals("Expected dbs ", Arrays.asList("db1", "db2", "db3"),
            dbs.stream().map(PolarisDatabaseEntity::getDbName).collect(Collectors.toList()));

        dbNames = getPolarisConnector().getDatabaseNames("db", null, 3);
        dbs = getPolarisConnector().getDatabases("db", null, 3);
        Assert.assertEquals("Expected dbNames ", Arrays.asList("db1", "db2", "db3"), dbNames);
        Assert.assertEquals("Expected dbs ", Arrays.asList("db1", "db2", "db3"),
            dbs.stream().map(PolarisDatabaseEntity::getDbName).collect(Collectors.toList()));

        dbNames = getPolarisConnector().getDatabaseNames("db", null, 4);
        dbs = getPolarisConnector().getDatabases("db", null, 4);
        Assert.assertEquals("Expected dbNames ", Arrays.asList("db1", "db2", "db3"), dbNames);
        Assert.assertEquals("Expected dbs ", Arrays.asList("db1", "db2", "db3"),
            dbs.stream().map(PolarisDatabaseEntity::getDbName).collect(Collectors.toList()));
    }
}
