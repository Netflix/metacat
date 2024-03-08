/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.server.usermetadata;

import com.netflix.metacat.common.server.model.Lookup;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

/**
 * Lookup service API.
 *
 * @author amajumdar
 */
public interface LookupService {
    /**
     * Returns the lookup for the given <code>name</code>.
     * If includeValues = true, we will set the Lookup values with the associated name, otherwise empty set
     *
     * @param name lookup name
     * @param includeValues whether we should set the values or not in the Lookup Object
     * @return lookup
     */
    @Nullable
    default Lookup get(final String name, final boolean includeValues) {
        return null;
    }

    /**
     * Returns the value of the lookup name.
     *
     * @param name lookup name
     * @return scalar lookup value
     */
    default String getValue(final String name) {
        return null;
    }

    /**
     * Returns the list of values of the lookup name.
     *
     * @param name lookup name
     * @return list of lookup values
     */
    default Set<String> getValues(final String name) {
        return Collections.emptySet();
    }

    /**
     * Returns the list of values of the lookup name.
     *
     * @param lookupId lookup id
     * @return list of lookup values
     */
    default Set<String> getValues(final Long lookupId) {
        return Collections.emptySet();
    }

    /**
     * Saves the lookup value.
     *
     * @param name   lookup name
     * @param values multiple values
     * @param includeValues whether to populate the values field in the Lookup Object
     * @return updated lookup
     */

    /**
     * Saves the lookup value.
     *
     * @param name   lookup name
     * @param values multiple values
     * @param includeValues whether to include values in the final Lookup Object
     * @return updated lookup
     */
    @Nullable
    default Lookup addValues(final String name, final Set<String> values, boolean includeValues) {
        return null;
    }

    /**
     * Saves the lookup value.
     *
     * @param name  lookup name
     * @param value lookup value
     * @param includeValues whether to return lookup value in the Lookup Object
     * @return updated lookup
     */
}
