package com.netflix.metacat.common.canonicaltype;

import java.util.List;

/**
 * Created by zhenli on 12/17/16.
 */
public interface ParametricType {
    String getName();
    Type createType(List<Type> types, List<Object> literals);
}
