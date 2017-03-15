package com.netflix.metacat.connector.hive.client.embedded;

import com.google.common.collect.Maps;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.metacat.common.util.DataSourceManager;
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
    public static final DynamicStringProperty JDO_TIMEOUT = DynamicPropertyFactory
            .getInstance().getStringProperty("metacat.jdo.timeout", "300000");
    private static Map<String, PersistenceManagerFactory> factories = Maps.newConcurrentMap();

    private HivePersistenceManagerFactory() {
    }

    /**
     * getPersistenceManagerFactory.
     * @param props props
     * @return PersistenceManagerFactory
     */
    public static PersistenceManagerFactory getPersistenceManagerFactory(final Map props) {
        final String name = String.valueOf(props.get("javax.jdo.option.name"));
        PersistenceManagerFactory result = factories.get(name);
        if (result == null) {
            result = getpersistencemanagerfactory(props);
        }
        return result;
    }

    private static synchronized PersistenceManagerFactory getpersistencemanagerfactory(final Map props) {
        final String name = String.valueOf(props.get("javax.jdo.option.name"));
        PersistenceManagerFactory result = factories.get(name);
        if (result == null) {
            final DataSource dataSource = DataSourceManager.get().get(name);
            final String jdoTimeout = JDO_TIMEOUT.get();
            final Map<String, Object> properties = Maps.newHashMap();
            properties.put("datanucleus.autoCreateSchema", props.get("datanucleus.autoCreateSchema"));
            properties.put("datanucleus.fixedDatastore", props.get("datanucleus.fixedDatastore"));
            properties.put("datanucleus.identifierFactory", "datanucleus1");
            properties.put("javax.jdo.option.DatastoreTimeout", jdoTimeout);
            properties.put("javax.jdo.option.DatastoreReadTimeoutMillis", jdoTimeout);
            properties.put("javax.jdo.option.DatastoreWriteTimeoutMillis", jdoTimeout);
            properties.put("datanucleus.ConnectionFactory", dataSource);
            properties.put("datanucleus.rdbms.useLegacyNativeValueStrategy", true);
            properties.put("datanucleus.transactionIsolation", "read-committed");
            properties.put("datanucleus.validateTables", false);
            properties.put("datanucleus.validateConstraints", false);
            properties.put("datanucleus.validateColumns", false);
            properties.put("datanucleus.cache.level2", false);
            properties.put("datanucleus.cache.level2.type", "none");
            properties.put("datanucleus.persistenceByReachabilityAtCommit", false);
            properties.put("datanucleus.autoStartMechanismMode", "Checked");
            properties.put("datanucleus.detachAllOnCommit", true);
            properties.put("datanucleus.detachAllOnRollback", true);
            result = JDOPersistenceManagerFactory.getPersistenceManagerFactory(properties);
            factories.put(name, result);
        }
        return result;
    }
}
