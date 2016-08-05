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

package com.netflix.metacat.common.partition.util

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Created by amajumdar on 3/1/16.
 */
class FilterPartitionSpec extends Specification{
    @Shared
    def filterPartition = new FilterPartition()

    @Unroll
    def 'evaluate expression #expression for name #name to #result'(){
        expect:
        filterPartition.evaluatePartitionExpression( expression, name, null) == result
        where:
        name                    | expression                                            | result
        "dateint=1"             | "dateint>1"                                           | false
        "dateint=1"             | "dateint>1 OR dateint<1"                              | false
        "dateint=1"             | "dateint>1 OR dateint=1"                              | true
        "dateint=1"             | "dateint>1 OR dateint = 1"                            | true
        "a=1"                   | "a=1"                                                 | true
        "a=1"                   | "A=1"                                                 | false
        "A=1"                   | "a=1"                                                 | false
        "dateint=1"             | "dateint>=1"                                          | true
        "dateint=1"             | "dateint<1"                                           | false
        "dateint=1"             | "dateint<=1"                                          | true
        "dateint=1"             | "dateint==1"                                          | true
        "dateint=-1"            | "dateint==-1"                                         | true
        "dateint=-1"            | "dateint== -1"                                        | true
        "dateint=1"             | "dateint==-1"                                         | false
        "dateint=-12"           | "dateint==-12"                                        | true
        "dateint=-12"           | "dateint== -12"                                       | true
        "dateint=12"            | "dateint>=-12"                                        | true
        "dateint=1"             | "dateint!=1"                                          | false
        "dateint=1"             | "(dateint>1) or (dateint<1)"                          | false
        "dateint=1"             | "(dateint>1) or (dateint<=1)"                         | true
        "dateint=1"             | "(dateint>1) and (dateint<=1)"                        | false
        "dateint=1"             | "(dateint==1) and ((dateint<=1) or (dateint>=1))"     | true
        "dateint=1"             | "(((dateint>1) or (dateint<=1)) and (dateint==1))"    | true
        "dateint=1"             | "('12' < 2)"                                          | false
        "dateint=1"             | "(12 < 2)"                                            | false
        "dateint=1"             | "('12' < '2')"                                        | true
        "dateint=1"             | "(12 < '2')"                                          | true
        "dateint=1"             | "(batchid>=1)"                                        | false

        "dateint=12"            | "(dateint < 2)"                                       | false
        "dateint=12"            | "(dateint <= 2)"                                      | false
        "dateint=12"            | "(dateint < '2')"                                     | true
        "dateint=12"            | "(dateint <= '2')"                                    | true
        "dateint=12"            | "(dateint2 != 2)"                                     | false
        "dateint=12"            | "(dateint2 == 12)"                                    | false

        "apath"                 | "(batchid>=1)"                                        | false

        "dateint=1/batchid=2"   | "(batchid>=1)"                                        | true
        "dateint=1/batchid=2"   | "((dateint==1) and (batchid>=1))"                     | true

        "dateint=1/type=java"   | "((dateint==1) and (type=='java'))"                   | true
        "dateint=1/type=java"   | "((dateint==1) and (type=='bava'))"                   | false

        "dateint=1/type=java"   | "(dateint>1 and type=='java') or (dateint==1 and type=='java')" | true
        "dateint=1/type=java"   | "(dateint>1 or dateint<1) and (type=='bava' or type=='java')" | false
        "dateint=1/type=java"   | "(dateint>1 or dateint<1) or (type=='bava' or type=='java')" | true
    }

    @Unroll
    def 'evaluate invalid expression #expression for name #name'(){
        when:
        filterPartition.evaluatePartitionExpression( expression, name, null)
        then:
        thrown(IllegalArgumentException)
        where:
        name                    | expression
        "dateint=1"             | "dateint>1?"
        "dateint=1"             | "dateint>1<<<"
        "a=1"                   | "1a=1"
        "dateint=1"             | "('12' <- 2)"
        "dateint=1"             | "(12 < 2))"
    }
}
