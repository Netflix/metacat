/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.util;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.StopStrategy;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * MetacatStopStrategy.
 *
 * @author zhenl
 * @since 1.0.0
 */
@NoArgsConstructor
@AllArgsConstructor
public class MetacatStopStrategy implements StopStrategy {
    private List<Class<? extends Exception>> predefinedexceptions = ImmutableList.<Class<? extends Exception>>builder()
            .add(NullPointerException.class)
            .add(IllegalStateException.class)
            .add(IllegalArgumentException.class)
            .build();
    private int maxAttempt;

    /**
     * Returns a stop strategy which stops after N failed attempts
     * or predefined exceptions.
     *
     * @param attemptNumber the number of failed attempts before stopping
     * @return a stop strategy
     */
    public MetacatStopStrategy stopAfterAttemptAndExceptions(final int attemptNumber) {
        return new MetacatStopStrategy(predefinedexceptions, attemptNumber);
    }


    /**
     * stopOn.
     *
     * @param classes classes
     * @return retryhelper
     */
    @SafeVarargs
    public final MetacatStopStrategy stopOn(@Nullable final Class<? extends Exception>... classes) {
        if (classes == null) {
            return this;
        }
        final List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(predefinedexceptions)
                .addAll(Arrays.asList(classes))
                .build();
        return new MetacatStopStrategy(exceptions, maxAttempt);

    }

    @Override
    public boolean shouldStop(final Attempt failedAttempt) {
        return failedAttempt.getAttemptNumber() >= maxAttempt
                || stopOnExceptions(failedAttempt.getExceptionCause());
    }

    private boolean stopOnExceptions(final Throwable throwable) {
        for (Class<? extends Exception> clazz : predefinedexceptions) {
            if (clazz.isInstance(throwable)) {
                return true;
            }
        }
        return false;
    }
}
