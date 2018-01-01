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
package jetbrains.exodus.vfs;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

/**
 * Converts raw data on read, and source data on write. E.g., raw data can be compressed or/and encrypted.
 * Source data is just content of a {@linkplain Cluster cluster}.
 *
 * @see VirtualFileSystem#getClusterConverter()
 * @see VirtualFileSystem#setClusterConverter(ClusterConverter)
 */
public interface ClusterConverter {

    /**
     * Converts raw (compressed or/and encrypted) data to source data.
     *
     * @param raw compressed or/and encrypted data
     * @return source data which is content of a cluster
     */
    @NotNull
    ByteIterable onRead(@NotNull final ByteIterable raw);

    /**
     * Converts source data (content of a cluster) to raw (compressed or/and encrypted) data.
     *
     * @param source content of a cluster
     * @return raw (compressed or/and encrypted) data
     */
    @NotNull
    ByteIterable onWrite(@NotNull final ByteIterable source);
}
