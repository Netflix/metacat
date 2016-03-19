package com.netflix.metacat.s3.connector.dao;

import com.netflix.metacat.s3.connector.model.Source;

/**
 * Created by amajumdar on 1/2/15.
 */
public interface SourceDao extends BaseDao<Source> {
    Source getByName(String name, boolean fromCache);
}
