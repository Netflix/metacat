/*
 *  Copyright 2019 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.common.server.usermetadata;

import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;

/**
 * Alias Service API.
 *
 * @author rveeramacheneni
 * @since 1.3.0
 */
public interface AliasService {
    /**
     * Returns the original table name if present.
     *
     * @param tableAlias the table alias.
     * @return the original name if present or the alias otherwise.
     */
    default QualifiedName getTableName(@NonNull final QualifiedName tableAlias) {
        return tableAlias;
    }
}
