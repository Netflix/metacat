package com.netflix.metacat.common.util;

import com.netflix.metacat.common.MetacatContext;

/**
 * Created by amajumdar on 8/3/15.
 */
public class MetacatContextManager {
    private static InheritableThreadLocal<MetacatContext> context = new InheritableThreadLocal<MetacatContext>();

    public static void removeContext() {
        context.remove();
    }

    public static void setContext(MetacatContext context) {
        MetacatContextManager.context.set(context);
    }

    public static MetacatContext getContext() {
        MetacatContext result = context.get();
        if(result == null) {
            result = new MetacatContext(null, null, null, null, null);
            setContext(result);
        }
        return result;
    }
}
