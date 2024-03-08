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
     * @return lookup
     */
    @Nullable
    default Lookup get(final String name) {
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
     * @return updated lookup
     */
    @Nullable
    default Lookup addValues(final String name, final Set<String> values) {
        return null;
    }

    /**
     * Saves the lookup value.
     *
     * @param name  lookup name
     * @param values lookup value
     * @return updated lookup
     */
    @Nullable
    default Lookup setValues(final String name, final Set<String> values) {
        return null;
    }

    /**
     * Saves the lookup value.
     *
     * @param name  lookup name
     * @param value lookup value
     * @return returns the lookup with the given name.
     */

    default Lookup setValue(final String name, final String value) {
        return null;
    }
}
