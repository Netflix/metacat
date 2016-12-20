package com.netflix.metacat.canonical.type;

import java.util.List;

/**
 * Created by zhenli on 12/17/16.
 */
public interface ParametricType {
    String getParametricTypeName();
    Type createType(List<Type> types, List<Object> literals);
}
