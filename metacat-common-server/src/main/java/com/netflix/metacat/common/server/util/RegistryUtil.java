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

package com.netflix.metacat.common.server.util;

import com.google.common.collect.Sets;
import com.netflix.metacat.common.QualifiedName;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Utility functions for Registry.
 * @author amajumdar
 */
public final class RegistryUtil {
    private static final String NAME_MONITORED_POOL = "MonitoredThreadPool_";

    private RegistryUtil() {
    }

    /**
     * Register the pool properties.
     *
     * @param registry micrometer registry
     * @param name     name of the monitor
     * @param pool     thread pool
     */
    public static void registerThreadPool(final MeterRegistry registry,
        final String name,
        final ThreadPoolExecutor pool) {
        registry.gauge(NAME_MONITORED_POOL + name + "_" + "activeCount", pool, ThreadPoolExecutor::getActiveCount);
        registry.gauge(NAME_MONITORED_POOL + name + "_" + "completedTaskCount", pool,
            ThreadPoolExecutor::getCompletedTaskCount);
        registry.gauge(NAME_MONITORED_POOL + name + "_" + "corePoolSize", pool, ThreadPoolExecutor::getCorePoolSize);
        registry
            .gauge(NAME_MONITORED_POOL + name + "_" + "maximumPoolSize", pool, ThreadPoolExecutor::getMaximumPoolSize);
        registry.gauge(NAME_MONITORED_POOL + name + "_" + "poolSize", pool, ThreadPoolExecutor::getPoolSize);
        registry.gaugeCollectionSize(NAME_MONITORED_POOL + name + "_" + "queueSize", Tags.empty(), pool.getQueue());
        registry.gauge(NAME_MONITORED_POOL + name + "_" + "taskCount", pool, ThreadPoolExecutor::getTaskCount);
    }

    /**
     * Convert a {@code QualifiedName} into a Set of {@code Tag}.
     *
     * @param name The QualifiedName
     * @return The Set of Meter Tags
     */
    public static Set<Tag> qualifiedNameToTagsSet(@Nonnull final QualifiedName name) {
        final Set<Tag> tagsSet = Sets.newHashSet();
        for (Map.Entry<String, String> part : name.parts().entrySet()) {
            tagsSet.add(Tag.of(part.getKey(), part.getValue()));
        }

        return tagsSet;
    }
}
