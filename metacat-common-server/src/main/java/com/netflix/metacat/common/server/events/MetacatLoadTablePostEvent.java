package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;

/**
 * Post table load event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatLoadTablePostEvent extends MetacatEvent {

    private final TableDto table;

    /**
     * Constructor.
     *
     * @param name           name
     * @param requestContext context
     * @param source         The source object which threw this event
     * @param table          table info
     */
    public MetacatLoadTablePostEvent(
        @Nonnull @NonNull final QualifiedName name,
        @Nonnull @NonNull final MetacatRequestContext requestContext,
        @Nonnull @NonNull final Object source,
        @Nonnull @NonNull final TableDto table
    ) {
        super(name, requestContext, source);
        this.table = table;
    }
}
