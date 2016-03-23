/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.partition.visitor

import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by amajumdar on 3/2/16.
 */
class PartitionParserEvalSpec extends Specification{
    @Shared def eval = new PartitionParserEval()

    def 'sql #sql to regex #regex'(){
        expect:
        eval.sqlLiketoRegexExpression(sql) == regex
        where:
        sql                     | regex
        "5[%]"                  | "5%"
        "[_]n"                  | "_n"
        "[a-cdf]"               | "[a-cdf]"
        "[-acdf]"               | "[-acdf]"
        "[[]"                   | "["
        "]"                     | "]"
        "abc[_]d%"              | "abc_d.*"
        "abc[def]"              | "abc[def]"
        "[def%]"                | "[def.*]"
        "[def_]"                | "[def.]"
        ""                      | ""
    }
}
