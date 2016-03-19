package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;

public interface DatabaseService {
    void create(QualifiedName name, DatabaseCreateRequestDto databaseCreateRequestDto);

    void update(QualifiedName name, DatabaseCreateRequestDto databaseCreateRequestDto);

    void delete(QualifiedName name);

    DatabaseDto get(QualifiedName name, boolean includeUserMetadata);

    boolean exists(QualifiedName name);
}
