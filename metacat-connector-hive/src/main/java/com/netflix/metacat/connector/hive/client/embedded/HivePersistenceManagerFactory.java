/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.client.embedded;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import javax.jdo.PersistenceManagerFactory;
import javax.sql.DataSource;
import java.util.Map;

/**
 * HivePersistenceManagerFactory.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public final class HivePersistenceManagerFactory {
    private static Map<String, PersistenceManagerFactory> factories = Maps.newConcurrentMap();

    private HivePersistenceManagerFactory() {
    }

    /**
     * getPersistenceManagerFactory.
     *
     * @param props props
     * @return PersistenceManagerFactory
     */
    public static PersistenceManagerFactory getPersistenceManagerFactory(final Map props) {
        final String name = String.valueOf(props.get(HiveConfigConstants.JAVAX_JDO_OPTION_NAME));
        PersistenceManagerFactory result = factories.get(name);
        if (result == null) {
            result = getpersistencemanagerfactory(props);
        }
        return result;
    }

    private static synchronized PersistenceManagerFactory getpersistencemanagerfactory(final Map props) {
        final String name = String.valueOf(props.get(HiveConfigConstants.JAVAX_JDO_OPTION_NAME));
        PersistenceManagerFactory result = factories.get(name);
        if (result == null) {
            final DataSource dataSource = DataSourceManager.get().get(name);
            final Map<String, Object> properties = Maps.newHashMap();
            properties.put(HiveConfigConstants.DATANUCLEUS_FIXEDDATASTORE,
                props.getOrDefault(HiveConfigConstants.DATANUCLEUS_FIXEDDATASTORE, true));
            properties.put(HiveConfigConstants.DATANUCLEUS_AUTOCREATESCHEMA,
                props.getOrDefault(HiveConfigConstants.DATANUCLEUS_AUTOCREATESCHEMA, false));
            properties.put(HiveConfigConstants.DATANUCLEUS_RDBMS_CHECKEXISTTABLESORVIEWS,
                props.getOrDefault(HiveConfigConstants.DATANUCLEUS_RDBMS_CHECKEXISTTABLESORVIEWS, false));
            properties.put(HiveConfigConstants.DATANUCLEUS_RDBMS_INITIALIZECOULUMNINFO,
                props.getOrDefault(HiveConfigConstants.DATANUCLEUS_RDBMS_INITIALIZECOULUMNINFO, "None"));
            properties.put(HiveConfigConstants.DATANUCLEUS_IDENTIFIERFACTORY,
                HiveConfigConstants.DATANUCLEUS_DATANUCLEU1);
            properties.put(HiveConfigConstants.DATANUCLEUS_CONNECTIONFACTORY, dataSource);
            properties.put(HiveConfigConstants.DATANUCLEUS_RDBMS_USELEGACYNATIVEVALUESTRATEGY, true);
            properties.put(HiveConfigConstants.DATANUCLEUS_TRANSACTIONISOLATION,
                HiveConfigConstants.DATANUCLEUS_READCOMMITTED);
            properties.put(HiveConfigConstants.DATANUCLEUS_VALIDATETABLE, false);
            properties.put(HiveConfigConstants.DATANUCLEUS_VALIDATECONSTRAINTS, false);
            properties.put(HiveConfigConstants.DATANUCLEUS_VALIDATECOLUMNS, false);
            properties.put(HiveConfigConstants.DATANUCLEUS_CACHE_LEVEL2, false);
            properties.put(HiveConfigConstants.DATANUCLEUS_CACHE_LEVEL2_TYPE, "none");
            properties.put(HiveConfigConstants.DATANUCLEUS_PERSISTENCYBYREACHATCOMMIT, false);
            properties.put(HiveConfigConstants.DATANUCLEUS_AUTOSTARTMECHANISMMODE, "Checked");
            properties.put(HiveConfigConstants.DATANUCLEUS_DETACHALLONCOMMIT, true);
            properties.put(HiveConfigConstants.DATANUCLEUS_DETACHALLONROLLBACK, true);
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT,
                props.get(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT));
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT,
                props.get(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT));
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT,
                props.get(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT));
            result = JDOPersistenceManagerFactory.getPersistenceManagerFactory(properties);
            factories.put(name, result);
        }
        return result;
    }
}
