package com.netflix.metacat.connector.polaris.store;


import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import java.util.Optional;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Random;

/**
 * Test persistence operations on Database objects.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaristest"})
@AutoConfigureDataJpa
public class PolarisStoreConnectorTest {
    private static final String DB_NAME_FOO = "foo";
    private static final String TBL_NAME_BAR = "bar";
    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private PolarisDatabaseRepository repo;

    @Autowired
    private PolarisTableRepository tblRepo;

    @Autowired
    private PolarisStoreConnector polarisConnector;

    private static String generateDatabaseName() {
        return DB_NAME_FOO + "_" + random.nextLong();
    }

    private static String generateTableName() {
        return TBL_NAME_BAR + "_" + random.nextLong();
    }

    private PolarisDatabaseEntity createDB(final String dbName) {
        final PolarisDatabaseEntity entity = polarisConnector.createDatabase(dbName);

        // assert that database exists, post-creation.
        Assert.assertTrue(polarisConnector.databaseExistsById(entity.getDbId()));
        Assert.assertTrue(polarisConnector.databaseExists(dbName));

        Assert.assertEquals(0L, entity.getVersion().longValue());
        Assert.assertTrue(entity.getDbId().length() > 0);
        Assert.assertEquals(dbName, entity.getDbName());

        final Optional<PolarisDatabaseEntity> fetchedEntity = polarisConnector.getDatabase(dbName);
        Assert.assertTrue(fetchedEntity.isPresent());
        Assert.assertEquals(entity, fetchedEntity.get());
        return entity;
    }

    private PolarisTableEntity createTable(final String dbName, final String tblName) {
        final PolarisTableEntity entity = polarisConnector.createTable(dbName, tblName);

        Assert.assertTrue(polarisConnector.tableExistsById(entity.getTblId()));
        Assert.assertTrue(polarisConnector.tableExists(dbName, tblName));

        Assert.assertTrue(entity.getTblId().length() > 0);
        Assert.assertTrue(entity.getVersion() >= 0);

        Assert.assertEquals(dbName, entity.getDbName());
        Assert.assertEquals(tblName, entity.getTblName());

        final Optional<PolarisTableEntity> fetchedEntity = polarisConnector.getTable(dbName, tblName);
        Assert.assertTrue(fetchedEntity.isPresent());
        Assert.assertEquals(entity, fetchedEntity.get());

        return entity;
    }

    /**
     * Test Database object creation and persistence.
     */
    @Test
    public void testCreateDB() {
        final PolarisDatabaseEntity savedEntity = createDB(generateDatabaseName());
    }

    /**
     * Test that a table cannot be created if database is absent.
     */
    @Test
    public void testTableCreationFailIfDatabaseIsAbsent() {
        Assertions.assertThrows(DataAccessException.class, () ->
            polarisConnector.createTable(generateDatabaseName(), generateTableName()));
    }

    /**
     * Test table creation if database exists.
     * Verify table deletion
     */
    @Test
    public void testTableCreationAndDeletion() {
        final String dbName = generateDatabaseName();
        final String tblName = generateTableName();
        final PolarisDatabaseEntity dbEntity = createDB(dbName);
        final PolarisTableEntity tblEntity = createTable(dbName, tblName);

        polarisConnector.deleteTable(dbName, tblName);
        Assert.assertFalse(polarisConnector.tableExistsById(tblEntity.getTblId()));
    }

    /**
     * Test to verify that table names fetch works.
     */
    @Test
    public void testPaginatedFetch() {
        final String dbName = generateDatabaseName();
        final PolarisDatabaseEntity dbEntity = createDB(dbName);
        List<String> tblNames = polarisConnector.getTables(dbName, "");
        Assert.assertEquals(0, tblNames.size());

        final String tblNameA = "A_" + generateTableName();
        final String tblNameB = "B_" + generateTableName();
        final String tblNameC = "C_" + generateTableName();
        createTable(dbName, tblNameA);
        createTable(dbName, tblNameB);
        createTable(dbName, tblNameC);

        tblNames = polarisConnector.getTables(dbName, "");
        Assert.assertEquals(3, tblNames.size());
        Assert.assertEquals(tblNameA, tblNames.get(0));
        Assert.assertEquals(tblNameB, tblNames.get(1));
        Assert.assertEquals(tblNameC, tblNames.get(2));
    }

    /**
     * Test to verify that table name can be updated.
     */
    @Test
    public void testTableUpdate() {
        // Create Table Entity in DB
        final String dbName = generateDatabaseName();
        final String tblName = generateTableName();
        final PolarisDatabaseEntity dbEntity = createDB(dbName);
        final PolarisTableEntity tblEntity = createTable(dbName, tblName);

        // Update table name
        final String newTblName = generateTableName();
        tblEntity.setTblName(newTblName);
        final PolarisTableEntity updatedTblEntity = polarisConnector.saveTable(tblEntity);
        Assert.assertEquals(newTblName, updatedTblEntity.getTblName());
    }

    /**
     * Test to validate that the table can be created via a PolarisTableEntity parameter.
     * Also tests that metadata_location is getting stored.
     */
    @Test
    public void createTableWithSaveApi() {
        final String dbName = generateDatabaseName();
        createDB(dbName);

        final String tblName = generateTableName();
        final String metadataLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/foo";
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tblName);
        e.setMetadataLocation(metadataLocation);

        final PolarisTableEntity savedEntity = polarisConnector.saveTable(e);
        Assert.assertEquals(metadataLocation, savedEntity.getMetadataLocation());
    }

    /**
     * Test to verify that compare-and-swap update of the metadata location works as expected.
     */
    @Test
    public void updateMetadataLocation() {
        final String dbName = generateDatabaseName();
        createDB(dbName);

        final String tblName = generateTableName();
        final String metadataLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/foo";
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tblName);
        e.setMetadataLocation(metadataLocation);
        final PolarisTableEntity savedEntity = polarisConnector.saveTable(e);

        final String newLocation = "s3/s3n://dataoven-prod/hive/dataoven_prod/warehouse/bar";

        // update should fail since the expected location is not going to match.
        boolean updatedSuccess = polarisConnector.updateTableMetadataLocation(
            dbName, tblName, "unexpected_location", newLocation);
        Assert.assertFalse(updatedSuccess);

        // successful update should happen.
        updatedSuccess = polarisConnector.updateTableMetadataLocation(dbName, tblName, metadataLocation, newLocation);
        Assert.assertTrue(updatedSuccess);


        // after the successful update, the same call should fail, since the current metadataLocation has changed.
        updatedSuccess = polarisConnector.updateTableMetadataLocation(dbName, tblName, metadataLocation, newLocation);
        Assert.assertFalse(updatedSuccess);
    }
}
