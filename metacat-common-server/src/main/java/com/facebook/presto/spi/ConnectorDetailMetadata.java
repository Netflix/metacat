package com.facebook.presto.spi;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by amajumdar on 1/15/15.
 */
public interface ConnectorDetailMetadata extends ConnectorMetadata {
    /**
     * Creates a schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schema schema metadata
     */
    void createSchema(ConnectorSession session, ConnectorSchemaMetadata schema);

    /**
     * Updates a schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schema schema metadata
     */
    void updateSchema(ConnectorSession session, ConnectorSchemaMetadata schema);

    /**
     * Drop a schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schemaName schema name
     */
    void dropSchema(ConnectorSession session, String schemaName);

    /**
     * Return schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schemaName schema name
     */
    ConnectorSchemaMetadata getSchema(ConnectorSession session, String schemaName);

    /**
     * Updates a table using the specified table metadata.
     */
    ConnectorTableHandle alterTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    /**
     * Returns all the table names referring to the given <code>uri</code>
     * @param uri location
     * @param prefixSearch if tru, we look for tables whose location starts with the given <code>uri</code>
     * @return list of table names
     */
    default List<SchemaTableName> getTableNames(String uri, boolean prefixSearch){
        return Lists.newArrayList();
    }

    /**
     * Similar to listTables but this method will return the list of tables along with its metadata.
     * @param session connector session
     * @param schemaName schema name
     * @return list of table metadata
     */
    List<ConnectorTableMetadata> listTableMetadatas(ConnectorSession session, String schemaName, List<String> tableNames);
}
