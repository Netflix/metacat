/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.common.server.properties;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;

import java.util.Set;

/**
 * Definition metadata related properties.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@lombok.Data
public class Definition {

    @NonNull
    private Metadata metadata = new Metadata();

    /**
     * Metadata related properties.
     *
     * @author amajumdar
     * @since 1.2.0
     */
    @lombok.Data
    public static class Metadata {

        @NonNull
        private Delete delete = new Delete();

        /**
         * Delete related properties.
         *
         * @author amajumdar
         * @since 1.2.0
         */
        @lombok.Data
        public static class Delete {
            private boolean enableForTable = true;
            private String enableDeleteForQualifiedNames;

            /**
             * Get the names stored in the variable as a List of fully qualified names.
             *
             * @return The names as a list or empty list if {@code enableDeleteForQualifiedNames} is null or empty
             */
            @JsonIgnore
            public Set<QualifiedName> getQualifiedNamesEnabledForDelete() {
                return PropertyUtils.delimitedStringsToQualifiedNamesSet(this.enableDeleteForQualifiedNames,
                    ',');
            }
        }
    }
}
