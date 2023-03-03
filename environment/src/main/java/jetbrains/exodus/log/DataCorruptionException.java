/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCorruptionException extends ExodusException {
    private static final Logger logger = LoggerFactory.getLogger(DataCorruptionException.class);


    DataCorruptionException(@NotNull final String message) {
        super(message);
    }

    private DataCorruptionException(@NotNull final String message, final long address, final long fileLengthBound) {
        this(message + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound));
    }

    public static void raise(@NotNull final String message, @NotNull final Log log, final long address) {
        checkLogIsClosing(log);
        log.switchToReadOnlyMode();
        var exception = new DataCorruptionException(message, address, log.getFileLengthBound());

        logger.error(message + LogUtil.getWrongAddressErrorMessage(address, log.getFileLengthBound()), exception);

        throw exception;
    }

    static void checkLogIsClosing(@NotNull final Log log) {
        if (log.isClosing()) {
            throw new IllegalStateException("Attempt to read closed log");
        }
    }
}
