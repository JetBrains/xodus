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

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

public final class NullLoggable {
    public static final byte TYPE = 0;

    public static SinglePageLoggable create(final long startAddress, final long endAddress) {
        return new SinglePageLoggable(startAddress, endAddress, TYPE, Loggable.NO_STRUCTURE_ID,
                Loggable.NULL_ADDRESS, ByteIterable.EMPTY_BYTES, 0, 0);
    }

    public static SinglePageLoggable create() {
        return SinglePageLoggable.NULL_PROTOTYPE;
    }

    public static boolean isNullLoggable(final byte type) {
        return type == TYPE;
    }

    public static boolean isNullLoggable(@NotNull final Loggable loggable) {
        return isNullLoggable(loggable.getType());
    }
}
