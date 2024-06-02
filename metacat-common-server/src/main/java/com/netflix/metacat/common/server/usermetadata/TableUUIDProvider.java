package com.netflix.metacat.common.server.usermetadata;

import com.netflix.metacat.common.dto.TableDto;

import java.util.Optional;

/**
 * Interface for TableUUIDPorvider.
 * @author yingjianw
 */
public interface TableUUIDProvider {
    /**
     * Given a tableDto, get the corresponding table uuid if applicable.
     * @param tableDto dto for table
     * @return the uuid of the table
     **/
    Optional<String> getUUID(TableDto tableDto);
}
