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

package com.netflix.metacat.common.server.partition.visitor

import com.netflix.metacat.common.server.partition.parser.PartitionParser
import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by amajumdar on 3/2/16.
 */
class PartitionParserEvalSpec extends Specification{
    @Shared def eval = new PartitionParserEval([dateint:20160418, app:'metacat'])

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

    def 'Expr #expr evaluated to #result'(){
        expect:
        new PartitionParser(new StringReader(expr)).filter().jjtAccept(eval, null) == result
        where:
        expr                                                                                                | result
        'dateint between 20160417 and 20160419'                                                             | true
        'dateint not between 20160417 and 20160419'                                                         | false
        'dateint between 20160427 and 20160429'                                                             | false
        'dateint in (20160417, 20160419, 20160418)'                                                         | true
        'dateint not in (20160417, 20160419, 20160418)'                                                     | false
        'dateint in (20160417, 20160419, 20160413)'                                                         | false
        "dateint between 20160417 and 20160419 and app=='metacat'"                                          | true
        "dateint between 20160417 and 20160419 and app=='metaca'"                                           | false
        "dateint between 20160417 and 20160419 or app=='metaca'"                                            | true
        "app like '%cat%'"                                                                                  | true
        "app not like '%cat%'"                                                                              | false
    }
}
