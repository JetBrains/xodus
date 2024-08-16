/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;

public class DataCorruptionException extends ExodusException {
    private static final ThreadLocal<Boolean> unsafeMode = ThreadLocal.withInitial(() -> Boolean.FALSE);

    public static void executeUnsafe(Runnable route) {
        unsafeMode.set(Boolean.TRUE);
        try {
            route.run();
        } finally {
            unsafeMode.set(Boolean.FALSE);
        }
    }

    public DataCorruptionException(@NotNull final String message) {
        super(message);
    }

    private DataCorruptionException(@NotNull final String message, final long address, final long fileLengthBound, final String logLocation) {
        this(message + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound) + ", database location: " + logLocation);
    }

    public static void raise(@NotNull final String message, @NotNull final Log log, final long address) {
        checkLogIsClosing(log);

        var unsafe = unsafeMode.get();
        if (!unsafe) {
            log.switchToReadOnlyMode();
        }

        throw new DataCorruptionException(message, address, log.getFileLengthBound(), log.getLocation());
    }

    static void checkLogIsClosing(@NotNull final Log log) {
        if (log.isClosing()) {
            throw new IllegalStateException("Attempt to read closed log");
        }
    }
}
