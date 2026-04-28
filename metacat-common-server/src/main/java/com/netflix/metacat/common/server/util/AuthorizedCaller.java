/*
 *
 *  Copyright 2024 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.server.util;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import javax.annotation.Nullable;

/**
 * An entry in a connector authorization allow list.
 * Matches on SsoDirectCallerAppName, and optionally on userName when {@link #userName} is set.
 *
 * @author jursetta
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public final class AuthorizedCaller {

    @NonNull
    private final String appName;

    @Nullable
    private final String userName;

    /**
     * Tests whether the given caller identity satisfies this rule.
     * appName must always match exactly. If this rule has a userName, the caller's userName must
     * match it exactly; if this rule has no userName, the caller's userName is ignored.
     *
     * @param callerAppName  the SsoDirectCallerAppName from the request
     * @param callerUserName the userName from the request (may be null)
     * @return true if the caller is allowed by this rule
     */
    public boolean matches(final String callerAppName, @Nullable final String callerUserName) {
        if (!appName.equals(callerAppName)) {
            return false;
        }
        if (userName == null) {
            return true;
        }
        return userName.equals(callerUserName);
    }
}
