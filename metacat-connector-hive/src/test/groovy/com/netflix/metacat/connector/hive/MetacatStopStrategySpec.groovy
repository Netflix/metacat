package com.netflix.metacat.connector.hive

import com.github.rholder.retry.Attempt
import com.github.rholder.retry.Retryer
import com.netflix.metacat.connector.hive.util.MetacatStopStrategy
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

/**
 * MetacatStopStrategySpec.
 * @author zhenl
 * @since 1.0.0
 */
class MetacatStopStrategySpec extends Specification{

    @Unroll
    def "Test for stop" (){
        given:
        def stop = new MetacatStopStrategy().stopAfterAttemptAndExceptions(3).shouldStop(
                attempt
        )
        expect:
        stop == expected

        where:
        attempt                                             | expected
        failedAttempt(3, new RuntimeException(), 1000L)     | true
        failedAttempt(2, new RuntimeException(), 1000L)     | false
        failedAttempt(1, new RuntimeException(), 1000L)     | false
        failedAttempt(1,new NullPointerException(), 0L)     | true
        failedAttempt(1,new IllegalStateException(), 0L)    | true
        failedAttempt(1,new IllegalArgumentException(), 0L) | true
    }

    @Unroll
    def "Test for stop with added exceptions" (){
        given:
        def stop = new MetacatStopStrategy()
                .stopAfterAttemptAndExceptions(3)
                .stopOn(exceptionclass).shouldStop(attempt)
        expect:
        stop == expected

        where:
        attempt                                            |exceptionclass | expected
        failedAttempt(3, new RuntimeException(), 1000L)     |TException.class | true
        failedAttempt(2, new RuntimeException(), 1000L)     |TException.class |false
        failedAttempt(1, new RuntimeException(), 1000L)     |TException.class |false
        failedAttempt(1,new NullPointerException(), 0L)     |TException.class |true
        failedAttempt(1,new IllegalStateException(), 0L)    |TException.class |true
        failedAttempt(1,new IllegalArgumentException(), 0L) |TException.class |true
        failedAttempt(1,new TException(), 0L)               |TException.class |true
        failedAttempt(1,null, 0L)                           |TException.class |false
        failedAttempt(3,null, 0L)                           |TException.class |true
        failedAttempt(2,new NullPointerException(), 0L)     |TException.class |true
        failedAttempt(2,null, 0L)                           | null            |false
    }

    Attempt<Boolean> failedAttempt(long attemptNumber, Exception exception, long delaySinceFirstAttempt) {
        return new Retryer.ExceptionAttempt<Boolean>(exception, attemptNumber, delaySinceFirstAttempt);
    }
}
