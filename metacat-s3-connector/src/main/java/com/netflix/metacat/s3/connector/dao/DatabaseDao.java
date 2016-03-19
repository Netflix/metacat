package com.netflix.metacat.s3.connector.dao;

import com.netflix.metacat.s3.connector.model.Database;

import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public interface DatabaseDao extends BaseDao<Database> {
    public Database getBySourceDatabaseName(String sourceName, String databaseName);
    public List<Database> getBySourceDatabaseNames(String sourceName, List<String> databaseNames);
}
