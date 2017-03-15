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
package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function

/**
 * Specifications for ConnectorUtils.
 *
 * @author tgianos
 * @since 1.0.0
 */
class ConnectorUtilsSpec extends Specification {

    @Shared
        a = QualifiedName.ofTable(UUID.randomUUID().toString(), "a", "c")

    @Shared
        b = QualifiedName.ofTable(UUID.randomUUID().toString(), "b", "b")

    @Shared
        c = QualifiedName.ofTable(UUID.randomUUID().toString(), "c", "a")

    @Shared
        dbComparator = Comparator.comparing(
            new Function<QualifiedName, String>() {
                @Override
                String apply(final QualifiedName qualifiedName) {
                    return qualifiedName.getDatabaseName()
                }
            }
        )

    @Shared
        tableComparator = Comparator.comparing(
            new Function<QualifiedName, String>() {
                @Override
                String apply(final QualifiedName qualifiedName) {
                    return qualifiedName.getTableName()
                }
            }
        )

    @Unroll
    "Can sort"() {
        expect:
        ConnectorUtils.sort(names, sort, comparator)
        names == expected

        where:
        names                    | comparator           | sort                          | expected
        [this.a, this.b, this.c] | this.dbComparator    | new Sort("d", SortOrder.DESC) | [this.c, this.b, this.a]
        [this.a, this.b, this.c] | this.dbComparator    | new Sort("d", SortOrder.ASC)  | [this.a, this.b, this.c]
        [this.a, this.b, this.c] | this.tableComparator | new Sort("d", SortOrder.DESC) | [this.a, this.b, this.c]
        [this.a, this.b, this.c] | this.tableComparator | new Sort("d", SortOrder.ASC)  | [this.c, this.b, this.a]
    }

    @Unroll
    "Can paginate"() {
        expect:
        ConnectorUtils.paginate(names, pagable) == expected

        where:
        names                    | pagable            | expected
        [this.a, this.b, this.c] | new Pageable(1, 2) | [this.c]
        [this.a, this.b, this.c] | new Pageable(2, 1) | [this.b, this.c]
        [this.a, this.b, this.c] | new Pageable(1, 1) | [this.b]
        [this.a, this.b, this.c] | new Pageable()     | [this.a, this.b, this.c]
        [this.a, this.b, this.c] | new Pageable(3, 0) | [this.a, this.b, this.c]
        [this.a, this.b, this.c] | new Pageable(3, 1) | [this.b, this.c]
    }
}
