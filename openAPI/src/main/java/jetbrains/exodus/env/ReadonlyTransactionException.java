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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;

/**
 * Is thrown on attempt to commit or flush read-only {@linkplain Transaction transaction} or if
 * {@linkplain Environment} is read-only, i.e. {@linkplain EnvironmentConfig#getEnvIsReadonly()} return {@code true}.
 *
 * @see Environment
 * @see Transaction
 * @see Transaction#isReadonly()
 * @see EnvironmentConfig#getEnvIsReadonly()
 * @see EnvironmentConfig#setEnvIsReadonly(boolean)
 */
public class ReadonlyTransactionException extends ExodusException {

    private static final String DEFAULT_MESSAGE = "Can't modify data in read-only transaction";

    public ReadonlyTransactionException() {
        this(DEFAULT_MESSAGE);
    }

    public ReadonlyTransactionException(@NotNull final String message) {
        super(message);
    }
}
