package com.netflix.metacat.common.server.usermetadata;

import com.netflix.metacat.common.dto.TableDto;

import java.util.Optional;

/**
 * Default Table UUID Provider.
 *
 * @author yingjianw
 */
public class DefaultTableUUIDProvider implements TableUUIDProvider {
    @Override
    public Optional<String> getUUID(final TableDto tableDto) {
        return Optional.empty();
    }
}
