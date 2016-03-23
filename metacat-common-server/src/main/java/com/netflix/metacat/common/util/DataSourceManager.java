/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.util;

import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSourceFactory;
import org.apache.tomcat.jdbc.pool.DataSourceProxy;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Driver;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amajumdar on 4/7/15.
 */
public class DataSourceManager {
    private static final String JDO_PREFIX = "javax.jdo.option.";
    private Map<String,DataSource> dataSources = Maps.newConcurrentMap();
    private static DataSourceManager instance = new DataSourceManager();

    private DataSourceManager(){}
    //
    // This method has been provided so that it can be used in the connectors. We could have injected into the plugins.
    //
    public static DataSourceManager get(){
        return instance;
    }

    public DataSourceManager load(String catalogName, Map<String, String> properties){
        if( dataSources.get(catalogName) == null){
            createDataSource( catalogName, properties);
        }
        return this;
    }

    public DataSourceManager load(String catalogName, Properties properties){
        if( dataSources.get(catalogName) == null){
            createDataSource( catalogName, properties);
        }
        return this;
    }

    public DataSource get(String catalogName){
        return dataSources.get(catalogName);
    }

    public Driver getDriver(String catalogName, Driver driver){
        DataSource dataSource = get(catalogName);
        return dataSource!=null? new JdbcDriver(driver, dataSource):driver;
    }

    private synchronized void createDataSource(String catalogName, Map props) {
        if( dataSources.get(catalogName) == null) {
            Properties dataSourceProperties = new Properties();
            props.forEach((key, value) -> {
                String prop = String.valueOf(key);
                if (prop.startsWith(JDO_PREFIX)) {
                    dataSourceProperties.put(prop.substring(JDO_PREFIX.length()), value);
                }
            });
            if( !dataSourceProperties.isEmpty()) {
                try {
                    DataSource dataSource = new DataSourceFactory().createDataSource(dataSourceProperties);
                    dataSources.put(catalogName, dataSource);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to load the data source for catalog %s with error [%s]", catalogName, e.getMessage()), e);
                }
            }
        }
    }

    @PreDestroy
    public void close(){
        Iterator<DataSource> iter = dataSources.values().iterator();
        while(iter.hasNext()){
            DataSourceProxy dataSource = (DataSourceProxy) iter.next();
            if( dataSource != null) {
                dataSource.close();
            }
            iter.remove();
        }
    }
}
