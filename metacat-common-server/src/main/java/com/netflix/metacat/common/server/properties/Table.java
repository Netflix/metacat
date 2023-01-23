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
package com.netflix.metacat.common.server.properties;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Splitter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Table related properties.
 *
 * @author tgianos
 * @since 1.1.0
 */
@lombok.Data
public class Table {

    @NonNull
    private Delete delete = new Delete();
    @NonNull
    private Rename rename = new Rename();
    @NonNull
    private Update update = new Update();

    /**
     * Delete related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @lombok.Data
    public static class Delete {

        @NonNull
        private Cascade cascade = new Cascade();
        private String noDeleteOnTags;
        private Set<String> noDeleteOnTagsSet;

        /**
         * Get the tags that disable table deletes.
         *
         * @return Set of tags
         */
        @JsonIgnore
        public Set<String> getNoDeleteOnTagsSet() {
            if (noDeleteOnTagsSet == null) {
                if (StringUtils.isNotBlank(noDeleteOnTags)) {
                    noDeleteOnTagsSet = new HashSet<>(Splitter.on(',')
                        .omitEmptyStrings()
                        .splitToList(noDeleteOnTags));
                } else {
                    noDeleteOnTagsSet = new HashSet<>();
                }
            }
            return noDeleteOnTagsSet;
        }

        /**
         * Cascade related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @lombok.Data
        public static class Cascade {

            @NonNull
            private Views views = new Views();

            /**
             * Views related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @lombok.Data
            public static class Views {
                private boolean metadata = true;
            }
        }
    }

    /**
     * Rename related properties.
     *
     * @author amajumdar
     * @since 1.3.0
     */
    @lombok.Data
    public static class Rename {

        private String noRenameOnTags;
        private Set<String> noRenameOnTagsSet;

        /**
         * Get the tags that disable table renames.
         *
         * @return Set of tags
         */
        @JsonIgnore
        public Set<String> getNoRenameOnTagsSet() {
            if (noRenameOnTagsSet == null) {
                if (StringUtils.isNotBlank(noRenameOnTags)) {
                    noRenameOnTagsSet = new HashSet<>(Splitter.on(',')
                        .omitEmptyStrings()
                        .splitToList(noRenameOnTags));
                } else {
                    noRenameOnTagsSet = new HashSet<>();
                }
            }
            return noRenameOnTagsSet;
        }
    }

    /**
     * Update related properties.
     */
    @lombok.Data
    public static class Update {

        private String noUpdateOnTags;
        private Set<String> noUpdateOnTagsSet;

        /**
         * Get the tags that disable table updates.
         *
         * @return Set of tags
         */
        @JsonIgnore
        public Set<String> getNoUpdateOnTagsSet() {
            if (noUpdateOnTagsSet == null) {
                if (StringUtils.isNotBlank(noUpdateOnTags)) {
                    noUpdateOnTagsSet = new HashSet<>(Splitter.on(',')
                            .omitEmptyStrings()
                            .trimResults()
                            .splitToList(noUpdateOnTags));
                } else {
                    noUpdateOnTagsSet = new HashSet<>();
                }
            }
            return noUpdateOnTagsSet;
        }
    }
}
