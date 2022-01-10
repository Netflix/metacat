
package com.netflix.metacat.connector.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteriaImpl;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpWrapper;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpsProxy;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.spectator.api.NoopRegistry;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spock.lang.Shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Test PolarisConnectorTableService.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polarisconnectortest"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureDataJpa
public class PolarisConnectorTableServiceTest {
    private static final String CATALOG_NAME = "catalog_name";
    private static final String DB_NAME = "db_name";
    private static final QualifiedName DB_QUALIFIED_NAME = QualifiedName.ofDatabase(CATALOG_NAME, DB_NAME);

    @Autowired
    private PolarisStoreService polarisStoreService;

    @Shared
    private ConnectorRequestContext requestContext = new ConnectorRequestContext();

    @Shared
    private ThreadServiceManager serviceManager = Mockito.mock(ThreadServiceManager.class);

    @Shared
    private ConnectorContext connectorContext;

    @Shared
    private PolarisConnectorDatabaseService polarisDBService;

    @Shared
    private PolarisConnectorTableService polarisTableService;

    /**
     * Initialization.
     */
    @BeforeEach
    public void init() {
        connectorContext = new ConnectorContext(CATALOG_NAME, CATALOG_NAME, "polaris",
            new DefaultConfigImpl(new MetacatProperties()), new NoopRegistry(), null,  Maps.newHashMap());
        polarisDBService = new PolarisConnectorDatabaseService(polarisStoreService);
        polarisTableService = new PolarisConnectorTableService(
            polarisStoreService,
            CATALOG_NAME,
            polarisDBService,
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            new IcebergTableHandler(connectorContext,
                new IcebergTableCriteriaImpl(connectorContext),
                new IcebergTableOpWrapper(connectorContext, serviceManager),
                new IcebergTableOpsProxy()),
            new PolarisTableMapper(CATALOG_NAME),
            connectorContext);
    }

    /**
     * Test empty list tables.
     */
    @Test
    public void testListTablesEmpty() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");
        final List<QualifiedName> names = polarisTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList());
    }

    /**
     * Test table exists.
     */
    @Test
    public void testTableExists() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        boolean exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertFalse(exists);
        polarisTableService.create(requestContext, tableInfo);
        exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertTrue(exists);
    }

    /**
     * Test table list.
     */
    @Test
    public void testList() {
        final QualifiedName name1 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(name1)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo1);
        final QualifiedName name2 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo2 = TableInfo.builder()
            .name(name2)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc2"))
            .build();
        polarisTableService.create(requestContext, tableInfo2);
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");
        final List<TableInfo> tables = polarisTableService.list(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(tables.size(), 2);
        Assert.assertEquals(tables.stream().map(TableInfo::getName).collect(Collectors.toSet()),
            ImmutableSet.of(name1, name2));
    }

    /**
     * Test table creation then list tables.
     */
    @Test
    public void testTableCreationAndList() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        final List<QualifiedName> names = polarisTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList(qualifiedName));
    }

    /**
     * Test multiple table creation then list tables.
     */
    @Test
    public void testMultipleTableCreationAndList() {
        final List<QualifiedName> createdTables = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table" + i);
            final TableInfo tableInfo = TableInfo.builder()
                .name(qualifiedName)
                .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc" + i))
                .build();
            polarisTableService.create(requestContext, tableInfo);
            createdTables.add(qualifiedName);
        }
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");
        final List<QualifiedName> names = polarisTableService.listNames(
            requestContext, DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC), new Pageable(20, 0));
        Assert.assertEquals(names, createdTables);
    }

    /**
     * Test table rename.
     */
    @Test
    public void testTableRename() {
        final QualifiedName nameOld = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final QualifiedName nameNew = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo = TableInfo.builder()
            .name(nameOld)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        boolean existsOld = polarisTableService.exists(requestContext, nameOld);
        Assert.assertTrue(existsOld);
        boolean existsNew = polarisTableService.exists(requestContext, nameNew);
        Assert.assertFalse(existsNew);
        polarisTableService.rename(requestContext, nameOld, nameNew);
        existsOld = polarisTableService.exists(requestContext, nameOld);
        Assert.assertFalse(existsOld);
        existsNew = polarisTableService.exists(requestContext, nameNew);
        Assert.assertTrue(existsNew);
    }

    /**
     * Test delete table.
     */
    @Test
    public void testDeleteTable() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table");
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        boolean exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertTrue(exists);
        polarisTableService.delete(requestContext, qualifiedName);
        exists = polarisTableService.exists(requestContext, qualifiedName);
        Assert.assertFalse(exists);
    }

    /**
     * Test get table using metadata json resource file.
     */
    @Test
    public void testGetTable() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final TableInfo tableInfo = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", location))
            .build();
        polarisTableService.create(requestContext, tableInfo);
        final TableInfo tableResult = polarisTableService.get(requestContext, qualifiedName);
        // check schema info correctly parsed from iceberg metadata file
        final List<FieldInfo> fields = tableResult.getFields();
        Assert.assertEquals(fields.size(), 3);
        Assert.assertEquals(fields.get(0).getName(), "id");
        Assert.assertEquals(fields.get(0).getComment(), "1st field");
        Assert.assertEquals(fields.get(0).getSourceType(), "long");
        Assert.assertEquals(fields.get(1).getName(), "data");
        Assert.assertEquals(fields.get(1).getComment(), "2nd field");
        Assert.assertEquals(fields.get(1).getSourceType(), "string");
        Assert.assertEquals(fields.get(2).getName(), "dateint");
        Assert.assertEquals(fields.get(2).getComment(), "3rd field");
        Assert.assertEquals(fields.get(2).getSourceType(), "int");
    }

    /**
     * Test update table reject cases.
     */
    @Test
    public void testUpdateTableReject() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final String location1 = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final String location2 = "src/test/resources/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json";
        final Map<String, String> metadata = new HashMap<>();
        metadata.put("metadata_location", location0);
        final TableInfo tableInfo0 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        // check update location without setting prev location fails
        metadata.put("metadata_location", location1);
        final TableInfo tableInfo1 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(InvalidMetaException.class,
            () -> polarisTableService.update(requestContext, tableInfo1));
        // check update location to new location equals blank fails
        metadata.put("previous_metadata_location", location0);
        metadata.put("metadata_location", "");
        final TableInfo tableInfo2 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(InvalidMetaException.class,
            () -> polarisTableService.update(requestContext, tableInfo2));
        // check update location existing and previous location do not match fails
        metadata.put("previous_metadata_location", location1);
        metadata.put("metadata_location", location2);
        final TableInfo tableInfo3 = TableInfo.builder().name(qualifiedName).metadata(metadata).build();
        Assertions.assertThrows(TablePreconditionFailedException.class,
            () -> polarisTableService.update(requestContext, tableInfo3));
    }

    /**
     * Test update table using metadata json resource file.
     */
    @Test
    public void testUpdateTableAccept() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final String location0 = "src/test/resources/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json";
        final TableInfo tableInfo0 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("metadata_location", location0))
            .build();
        polarisTableService.create(requestContext, tableInfo0);
        final TableInfo tableResult0 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult0.getMetadata().get("metadata_location"), location0);
        final String location1 = "src/test/resources/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json";
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(qualifiedName)
            .metadata(ImmutableMap.of("previous_metadata_location", location0, "metadata_location", location1))
            .build();
        polarisTableService.update(requestContext, tableInfo1);
        final TableInfo tableResult1 = polarisTableService.get(requestContext, qualifiedName);
        Assert.assertEquals(tableResult1.getMetadata().get("metadata_location"), location1);
    }
}

