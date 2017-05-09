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
public final class HivePersistenceManagerFactory {
    /**
     * metacat.jdo.timeout.
     */
    private static final String JDO_TIMEOUT = "300000";

    //TODO: Figure out how to do this statically in the new Spring based system
//    public static final DynamicStringProperty JDO_TIMEOUT = DynamicPropertyFactory
//            .getInstance().getStringProperty(HiveConfigConstants.METACAT_JDO_TIMEOUT, "300000");
    private static Map<String, PersistenceManagerFactory> factories = Maps.newConcurrentMap();

    private HivePersistenceManagerFactory() {
    }

    /**
     * getPersistenceManagerFactory.
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
//            final String jdoTimeout = JDO_TIMEOUT.get();
            final Map<String, Object> properties = Maps.newHashMap();
            properties.put(HiveConfigConstants.DATANUCLEUS_FIXEDDATASTORE,
                    props.get(HiveConfigConstants.DATANUCLEUS_FIXEDDATASTORE));
            properties.put(HiveConfigConstants.DATANUCLEUS_AUTOCREATESCHEMA,
                    props.get(HiveConfigConstants.DATANUCLEUS_AUTOCREATESCHEMA));
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
//            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, jdoTimeout);
//            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, jdoTimeout);
//            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, jdoTimeout);
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, JDO_TIMEOUT);
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, JDO_TIMEOUT);
            properties.put(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, JDO_TIMEOUT);
            result = JDOPersistenceManagerFactory.getPersistenceManagerFactory(properties);
            factories.put(name, result);
        }
        return result;
    }
}
