package com.netflix.metacat.connector.polaris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
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
import java.util.stream.Collectors;

/**
 * Test PolarisConnectorTableService in functional test.
 * Some of the tests cannot be run in unit test as it uses h2 database, which does not support all
 * functionalities in crdb so include those tests here.
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
public class PolarisConnectorTableServiceFunctionalTest extends PolarisConnectorTableServiceTest {
    /**
     * Test get table names.
     */
    @Test
    public void testGetTableNames() {
        final QualifiedName name1 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(name1)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        getPolarisTableService().create(getRequestContext(), tableInfo1);
        final QualifiedName name2 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo2 = TableInfo.builder()
            .name(name2)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc2"))
            .build();
        getPolarisTableService().create(getRequestContext(), tableInfo2);
        final QualifiedName name3 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table3");
        final TableInfo tableInfo3 = TableInfo.builder()
            .name(name3)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc3"))
            .build();
        getPolarisTableService().create(getRequestContext(), tableInfo3);

        TestUtil.simulateDelay();

        final List<QualifiedName> tables = getPolarisTableService()
            .getTableNames(getRequestContext(), DB_QUALIFIED_NAME, "", -1);
        Assert.assertEquals(tables.size(), 3);
        Assert.assertEquals(tables, ImmutableList.of(name1, name2, name3));
    }

    /**
     * Test empty list tables.
     */
    @Test
    public void testListTablesEmpty() {
        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");

        TestUtil.simulateDelay();

        final List<QualifiedName> names = getPolarisTableService().listNames(
            getRequestContext(), DB_QUALIFIED_NAME, qualifiedName,
            new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList());
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
        getPolarisTableService().create(getRequestContext(), tableInfo);

        TestUtil.simulateDelay();

        final List<QualifiedName> names = getPolarisTableService().listNames(
            getRequestContext(), DB_QUALIFIED_NAME, qualifiedName,
            new Sort(null, SortOrder.ASC), new Pageable(2, 0));
        Assert.assertEquals(names, Arrays.asList(qualifiedName));
    }

    /**
     * Test table list.
     */
    @Test
    @Disabled("Skipping")
    public void testList() {
        final QualifiedName name1 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table1");
        final TableInfo tableInfo1 = TableInfo.builder()
            .name(name1)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc1"))
            .build();
        this.getPolarisTableService().create(this.getRequestContext(), tableInfo1);
        final QualifiedName name2 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo2 = TableInfo.builder()
            .name(name2)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc2"))
            .build();
        this.getPolarisTableService().create(this.getRequestContext(), tableInfo2);


        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");

        TestUtil.simulateDelay();

        List<TableInfo> tables = this.getPolarisTableService().list(
            this.getRequestContext(), DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC),
            new Pageable(2, 0));
        Assert.assertEquals(tables.size(), 2);
        Assert.assertEquals(tables.stream().map(TableInfo::getName).collect(Collectors.toSet()),
            ImmutableSet.of(name1, name2));

        // Create a 3rd table, but this time does not sleep so this table should not be included
        final QualifiedName name3 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table3");
        final TableInfo tableInfo3 = TableInfo.builder()
            .name(name3)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc2"))
            .build();
        this.getPolarisTableService().create(this.getRequestContext(), tableInfo3);

        tables = this.getPolarisTableService().list(
            this.getRequestContext(), DB_QUALIFIED_NAME, qualifiedName, new Sort(null, SortOrder.ASC),
            new Pageable(3, 0));
        Assert.assertEquals(tables.size(), 2);
        Assert.assertEquals(tables.stream().map(TableInfo::getName).collect(Collectors.toSet()),
            ImmutableSet.of(name1, name2));
    }
}
