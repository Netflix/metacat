package com.netflix.metacat.s3.connector.dao;

import com.netflix.metacat.s3.connector.model.Table;

import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public interface TableDao extends BaseDao<Table> {
    public Table getBySourceDatabaseTableName(String sourceName, String schemaName, String tableName);
    public List<Table> getBySourceDatabaseTableNames(String sourceName, String schemaName, List<String> tableNames);
}