package com.netflix.metacat.connector.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test PolarisConnectorTableService in functional test.
 * Some of the tests cannot be run in unit test as it uses h2 database, which does not support all
 * functionalities in crdb so include those tests here.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PolarisPersistenceConfig.class})
@ActiveProfiles(profiles = {"polaris_functional_test"})
@AutoConfigureDataJpa
public class PolarisConnectorTableServiceFunctionalTest extends PolarisConnectorTableServiceTest {
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
        this.getPolarisTableService().create(this.getRequestContext(), tableInfo1);
        final QualifiedName name2 = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "table2");
        final TableInfo tableInfo2 = TableInfo.builder()
            .name(name2)
            .metadata(ImmutableMap.of("table_type", "ICEBERG", "metadata_location", "loc2"))
            .build();
        this.getPolarisTableService().create(this.getRequestContext(), tableInfo2);


        final QualifiedName qualifiedName = QualifiedName.ofTable(CATALOG_NAME, DB_NAME, "");

        try {
            // pause execution for 10000 milliseconds (10 seconds)
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Sleep was interrupted");
        }

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
            new Pageable(2, 0));
        Assert.assertEquals(tables.size(), 2);
        Assert.assertEquals(tables.stream().map(TableInfo::getName).collect(Collectors.toSet()),
            ImmutableSet.of(name1, name2));
    }
}
