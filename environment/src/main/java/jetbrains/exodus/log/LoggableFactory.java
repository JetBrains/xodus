/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

public abstract class LoggableFactory {

    private static final int MAXIMUM_LOGGABLE_TYPE = 128;
    private static final LoggableFactory[] FACTORY = new LoggableFactory[MAXIMUM_LOGGABLE_TYPE];

    protected LoggableFactory() {
    }

    public static void registerLoggable(final int type, final LoggableFactory loggableFactory) {
        final LoggableFactory prevLoggableFactory = FACTORY[type];
        if (prevLoggableFactory != null && prevLoggableFactory != loggableFactory) {
            throw new ExodusException("Loggable type is already registered " + type);
        }
        FACTORY[type] = loggableFactory;
    }

    public static void clear() {
        for (int i = 0; i < FACTORY.length; i++) {
            FACTORY[i] = null;
        }
    }

    static LoggableFactory getPrototype(final byte type) {
        if (type < 0 || type >= FACTORY.length) {
            throw new ExodusException("Invalid loggable type");
        }
        return FACTORY[type];
    }

    /**
     * Creates new instance of loggable.
     *
     * @param address     address of result in the log.
     * @param data        loggable data.
     * @param dataLength  length of the loggable data.
     * @param structureId structure id of loggable.
     * @return new loggable instance.
     */
    protected abstract RandomAccessLoggable create(final long address,
                                                   @NotNull final ByteIterableWithAddress data,
                                                   final int dataLength,
                                                   final int structureId);
}
