package com.netflix.metacat.main.manager

import com.facebook.presto.metadata.QualifiedTableName
import com.facebook.presto.metadata.QualifiedTablePrefix
import com.google.inject.Inject
import com.netflix.metacat.main.presto.metadata.MetadataManager
import spock.lang.Ignore

/**
 * Created by amajumdar on 1/15/15.
 */
class PluginManagerSpec extends BaseSpec {
    @Inject
    PluginManager pluginManager
    @Inject
    MetadataManager metadataManager

    def testLoadPlugins() {
        when:
        pluginManager.loadPlugins()
        then:
        notThrown(Exception)
    }

    def testMetadataManagerMySql() {
        when:
        metadataManager.listSchemaNames(TEST_MYSQL_SESSION, "default")
        metadataManager.listTables(TEST_SESSION, new QualifiedTablePrefix("default", "example"))
        def handle = metadataManager.getTableHandle(TEST_MYSQL_SESSION, QualifiedTableName.valueOf("default.example.numbers"))
        metadataManager.getTableMetadata(TEST_MYSQL_SESSION, handle.get())
        metadataManager.getCatalogNames()
        then:
        notThrown(Exception)
    }

    def testMetadataManagerExample() {
        when:
        metadataManager.listSchemaNames(TEST_SESSION, "example")
        metadataManager.listTables(TEST_SESSION, new QualifiedTablePrefix("example", "example"))
        def handle = metadataManager.getTableHandle(TEST_SESSION, QualifiedTableName.valueOf("example.example.numbers"))
        metadataManager.getTableMetadata(TEST_SESSION, handle.get())
        metadataManager.getCatalogNames()
        then:
        notThrown(Exception)
    }

    @Ignore
    def testMetadataManagerHive() {
        when:
        metadataManager.listSchemaNames(TEST_HIVE_SESSION, "testhive")
        metadataManager.listTables(TEST_HIVE_SESSION, new QualifiedTablePrefix("testhive", "charsmith"))
        def handle = metadataManager.getTableHandle(TEST_HIVE_SESSION, QualifiedTableName.valueOf("testhive.charsmith.temp"))
        metadataManager.getTableMetadata(TEST_HIVE_SESSION, handle.get())
        metadataManager.getCatalogNames()
        then:
        notThrown(Exception)
    }

    @Ignore
    def testMetadataManagerFranklin() {
        when:
        metadataManager.listSchemaNames(TEST_FRANKLIN_SESSION, "s3")
        metadataManager.listTables(TEST_HIVE_SESSION, new QualifiedTablePrefix("s3", "charsmith"))
        def handle = metadataManager.getTableHandle(TEST_HIVE_SESSION, QualifiedTableName.valueOf("s3.charsmith.part"))
        metadataManager.getTableMetadata(TEST_HIVE_SESSION, handle.get())
        metadataManager.getCatalogNames()
        then:
        notThrown(Exception)
    }
}
