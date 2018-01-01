/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus;

import jetbrains.exodus.entitystore.EntityStoreException;
import org.jetbrains.annotations.Nullable;

/**
 * Any exodus exception is {@code ExodusException}.
 */
public class ExodusException extends RuntimeException {

    public ExodusException() {
    }

    public ExodusException(String message) {
        super(message);
    }

    public ExodusException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExodusException(Throwable cause) {
        super(cause);
    }

    public static ExodusException wrap(Exception e) {
        return e instanceof ExodusException ? (ExodusException) e : new ExodusException(e);
    }

    public static RuntimeException toExodusException(final Throwable e, @Nullable final String message) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return message == null ? new ExodusException(e) : new ExodusException(message, e);
    }

    public static RuntimeException toExodusException(Throwable e) {
        return toExodusException(e, null);
    }

    public static RuntimeException toEntityStoreException(Throwable e) {
        return e instanceof RuntimeException ? (RuntimeException) e : new EntityStoreException(e);
    }
}
