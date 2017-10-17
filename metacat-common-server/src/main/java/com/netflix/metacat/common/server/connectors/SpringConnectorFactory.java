/*
 *
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
 *
 */

package com.netflix.metacat.common.server.connectors;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/**
 * Spring based Connector Factory.
 *
 * @author zhenl
 * @since 1.1.0
 */

public abstract class SpringConnectorFactory implements ConnectorFactory {
    protected final String catalogName;
    protected final AnnotationConfigApplicationContext ctx;

    /**
     * Constructor.
     *
     * @param catalogName            catalog name
     * @param connectorInfoConverter connector info converter
     * @param connectorContext       connector related config
     */
    public SpringConnectorFactory(final String catalogName,
                                  final ConnectorInfoConverter connectorInfoConverter,
                                  final ConnectorContext connectorContext) {
        this.catalogName = catalogName;
        this.ctx = new AnnotationConfigApplicationContext();
        this.ctx.setEnvironment(new StandardEnvironment());
        this.ctx.getBeanFactory().registerSingleton("ConnectorContext", connectorContext);
        this.ctx.getBeanFactory().registerSingleton("ConnectorInfoConverter", connectorInfoConverter);
    }

    /**
     * registerclasses to context.
     * Known issue: can not register the two beans that are the same class but have different qualifiers
     * @param clazz classes object.
     */
    protected void registerClazz(final Class<?>... clazz) {
        this.ctx.register(clazz);
    }

    /**
     * Add property source to env.
     *
     * @param properties Property source for enviroment.
     */
    protected void addEnvProperties(final MapPropertySource properties) {
        this.ctx.getEnvironment().getPropertySources().addFirst(properties);
    }

    /**
     * refresh the context.
     */
    public void refresh() {
        this.ctx.refresh();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        this.ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.catalogName;
    }
}
