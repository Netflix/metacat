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
