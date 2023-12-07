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

package com.netflix.metacat.common.server.partition.util;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.partition.parser.ParseException;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.common.server.partition.parser.TokenMgrError;
import com.netflix.metacat.common.server.partition.visitor.PartitionParserEval;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 * Partition filter utility.
 */
@Slf4j
public class FilterPartition {
    private PartitionParser parser;
    private Map<String, String> context = Maps.newLinkedHashMap();

    /**
     * Evaluates the given expression.
     * @param partitionExpression expression
     * @param name name
     * @param path partition uri path
     * @return true if the expression evaluates to true
     * @throws IOException exception
     */

    public boolean evaluatePartitionExpression(final String partitionExpression, final String name, final String path)
        throws IOException {
        return evaluatePartitionExpression(partitionExpression, name, path, false, null);
    }

    /**
     * Evaluates the given expression.
     * @param partitionExpression expression
     * @param name name
     * @param path partition uri path
     * @param batchid batch id
     * @param values map of values
     * @return true if the expression evaluates to true
     */
    @SuppressFBWarnings(value = "DCN_NULLPOINTER_EXCEPTION", justification = "keep the original logic")
    public boolean evaluatePartitionExpression(final String partitionExpression, final String name, final String path,
        final boolean batchid, final Map<String, String> values) {
        if (partitionExpression != null) {
            try {
                if (parser == null) {
                    parser = new PartitionParser(new StringReader(partitionExpression));
                } else {
                    parser.ReInit(new StringReader(partitionExpression));
                }
                context.clear();
                if (batchid) {
                    addPathValues(path, context);
                }
                addNameValues(name, context);
                if (values != null) {
                    context.putAll(values);
                }
                if (context.size() > 0) {
                    return (Boolean) parser.filter().jjtAccept(new PartitionParserEval(context), null);
                } else {
                    return false;
                }
            } catch (ParseException | TokenMgrError e) {
                throw new IllegalArgumentException(String.format("Invalid expression: %s", partitionExpression), e);
            } catch (StackOverflowError | ArrayIndexOutOfBoundsException | NullPointerException e) {
                throw new IllegalArgumentException(String.format("Expression too long: %s", partitionExpression), e);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Throwable t) {
                log.warn("Caught unexpected exception during evaluatePartitionExpression", t);
                return false;
            }
        }
        return true;
    }

    /**
     * Adds the path part values to context.
     * @param path location
     * @param values Map of part keys and values.
     */
    protected void addPathValues(final String path, final Map<String, String> values) {
        PartitionUtil.getPartitionKeyValues(path, values);
    }
    /**
     * Adds the name part values to context.
     * @param name part name
     * @param values Map of part keys and values.
     */
    protected void addNameValues(final String name, final Map<String, String> values) {
        PartitionUtil.getPartitionKeyValues(name, values);
    }
}
