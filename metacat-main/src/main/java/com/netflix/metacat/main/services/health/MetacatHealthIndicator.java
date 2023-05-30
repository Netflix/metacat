package com.netflix.metacat.main.services.health;

import com.netflix.metacat.main.services.init.MetacatCoreInitService;
import com.netflix.metacat.main.services.init.MetacatThriftInitService;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.transport.TSocket;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Metacat health indicator.
 */
@RequiredArgsConstructor
public class MetacatHealthIndicator implements HealthIndicator {
    protected static final String PLUGIN_KEY = "pluginsLoaded";
    protected static final String CATALOG_KEY = "catalogsLoaded";
    protected static final String THRIFT_KEY = "thriftStarted";

    private final MetacatCoreInitService coreInitService;
    private final MetacatThriftInitService thriftInitService;

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final boolean plugins = coreInitService.pluginManager().arePluginsLoaded();
        final boolean catalogs = coreInitService.catalogManager().areCatalogsLoaded();
        final boolean thrift = thriftInitService.thriftStarted().get()
                                   && thriftInitService.metacatThriftService()
                                          .getCatalogThriftServices().parallelStream().map(c -> {
            TSocket transport = null;
            try {
                transport = new TSocket("localhost", c.getPortNumber(), 100);
                transport.open();
            } catch (Exception e) {
                return false;
            } finally {
                if (transport != null && transport.isOpen()) {
                    transport.close();
                }
            }
            return true;
        }).reduce(Boolean.TRUE, Boolean::equals);

        final Health.Builder builder = plugins && catalogs && thrift ? Health.up() : Health.outOfService();

        builder.withDetail(PLUGIN_KEY, plugins);
        builder.withDetail(CATALOG_KEY, catalogs);
        builder.withDetail(THRIFT_KEY, thrift);

        return builder.build();
    }
}
