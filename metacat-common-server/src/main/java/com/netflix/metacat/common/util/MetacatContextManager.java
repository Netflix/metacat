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

package com.netflix.metacat.common.util;

import com.netflix.metacat.common.MetacatRequestContext;

/**
 * Created by amajumdar on 8/3/15.
 */
public class MetacatContextManager {
    private static InheritableThreadLocal<MetacatRequestContext> context = new InheritableThreadLocal<MetacatRequestContext>();

    public static void removeContext() {
        context.remove();
    }

    public static void setContext(MetacatRequestContext context) {
        MetacatContextManager.context.set(context);
    }

    public static MetacatRequestContext getContext() {
        MetacatRequestContext result = context.get();
        if(result == null) {
            result = new MetacatRequestContext(null, null, null, null, null);
            setContext(result);
        }
        return result;
    }
}
