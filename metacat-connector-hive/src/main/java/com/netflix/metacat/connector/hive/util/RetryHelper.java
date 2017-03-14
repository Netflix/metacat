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

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RetryHelper.
 *
 * @author zhenl
 */
@Slf4j
public final class RetryHelper {
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.ofSeconds(30);
    private static final double DEFAULT_SCALE_FACTOR = 2.0;

    private final int maxAttempts;
    private final Duration minSleepTime;
    private final Duration maxSleepTime;
    private final double scaleFactor;
    private final Duration maxRetryTime;
    private final Function<Exception, Exception> exceptionMapper;
    private final List<Class<? extends Exception>> exceptionWhiteList;
    private final Optional<Runnable> retryRunnable;

    private RetryHelper(
            final int maxAttempts,
            final Duration minSleepTime,
            final Duration maxSleepTime,
            final double scaleFactor,
            final Duration maxRetryTime,
            final Function<Exception, Exception> exceptionMapper,
            final List<Class<? extends Exception>> exceptionWhiteList,
            final Optional<Runnable> retryRunnable) {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.exceptionMapper = exceptionMapper;
        this.exceptionWhiteList = exceptionWhiteList;
        this.retryRunnable = retryRunnable;
    }

    private RetryHelper() {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                Function.identity(),
                ImmutableList.of(),
                Optional.empty());
    }

    /**
     * retry.
     *
     * @return retryhelper
     */
    public static RetryHelper retry() {
        return new RetryHelper();
    }

    /**
     * maxAttemps.
     *
     * @param maxAttempt maxAttempts
     * @return retryhelper
     */
    public RetryHelper maxAttempts(final int maxAttempt) {
        return new RetryHelper(maxAttempt, minSleepTime, maxSleepTime,
                scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    /**
     * exponentialBackoff.
     *
     * @param minSleepTimes minSleepTime
     * @param maxSleepTimes maxSleepTime
     * @param maxRetryTimes maxRetryTime
     * @param scalefactor   scaleFactor
     * @return retryhelper
     */
    public RetryHelper exponentialBackoff(final Duration minSleepTimes,
                                          final Duration maxSleepTimes,
                                          final Duration maxRetryTimes,
                                          final double scalefactor) {
        return new RetryHelper(maxAttempts, minSleepTimes,
                maxSleepTimes, scalefactor, maxRetryTimes, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    /**
     * onRetry.
     *
     * @param retryRunnables retryrunnable
     * @return retryhelper
     */
    public RetryHelper onRetry(final Runnable retryRunnables) {
        return new RetryHelper(maxAttempts, minSleepTime,
                maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper,
                exceptionWhiteList, Optional.ofNullable(retryRunnables));
    }

    /**
     * exceptionMapper.
     *
     * @param exceptionMappers exceptionMapper
     * @return retryhelper
     */
    public RetryHelper exceptionMapper(final Function<Exception, Exception> exceptionMappers) {
        return new RetryHelper(maxAttempts, minSleepTime,
                maxSleepTime, scaleFactor, maxRetryTime, exceptionMappers, exceptionWhiteList, retryRunnable);
    }

    /**
     * stopOn.
     *
     * @param classes classes
     * @return retryhelper
     */
    @SafeVarargs
    public final RetryHelper stopOn(@NonNull final Class<? extends Exception>... classes) {
        final List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhiteList)
                .addAll(Arrays.asList(classes))
                .build();

        return new RetryHelper(maxAttempts,
                minSleepTime, maxSleepTime, scaleFactor,
                maxRetryTime, exceptionMapper, exceptions, retryRunnable);
    }

    /**
     * stopOnIllegalExceptions.
     *
     * @return retryhelper
     */
    public RetryHelper stopOnIllegalExceptions() {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    /**
     * run.
     *
     * @param callableName callableName
     * @param callable     callable
     * @param <V>          v
     * @return v
     * @throws Exception exception
     */
    public <V> V run(@NonNull final String callableName, @NonNull final Callable<V> callable)
            throws Exception {
        final long startTime = System.currentTimeMillis();
        int attempt = 0;
        while (true) {
            attempt++;

            if (attempt > 1) {
                retryRunnable.ifPresent(Runnable::run);
            }

            try {
                return callable.call();
            } catch (Exception e) {
                e = exceptionMapper.apply(e);
                for (Class<? extends Exception> clazz : exceptionWhiteList) {
                    if (clazz.isInstance(e)) {
                        throw e;
                    }
                }
                if (attempt >= maxAttempts || (System.currentTimeMillis() - startTime - maxRetryTime.toMillis()) >= 0) {
                    throw e;
                }
                log.debug("Failed on executing %s with attempt %d,"
                        + " will retry. Exception: %s", callableName, attempt, e.getMessage());

                final int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempt - 1),
                        maxSleepTime.toMillis());
                TimeUnit.MILLISECONDS.sleep(delayInMs);
            }
        }
    }

    /**
     * callableWrap.
     * @param callable callable
     * @param <V> v
     * @return callable
     */
    public static  <V> Callable<V> callableWrap(final Callable<V> callable) {
        return new Callable<V>() {
            @Override
            public V call() throws Exception {
                return callable.call();
            }
        };
    }

}
