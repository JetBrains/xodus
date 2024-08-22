/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.io;

import org.jetbrains.annotations.NotNull;

/**
 * {@code DataReader} defines basic structure of {@code Log}. {@code DataReader} provides access to all {@linkplain
 * Block blocks} or groups of successive {@linkplain Block blocks} in the log.
 *
 * @see Block
 * @see DataWriter
 * @see DataReaderWriterProvider
 * @since 1.3.0
 */
public interface DataReader {

    /**
     * Returns {@code Log} location how it was passed as a parameter to {@linkplain DataReaderWriterProvider#newReaderWriter(String)}.
     *
     * @return location how it was passed as a parameter to {@linkplain DataReaderWriterProvider#newReaderWriter(String)}
     * @see DataReaderWriterProvider#newReaderWriter(String)
     */
    @NotNull
    String getLocation();

    /**
     * Returns all {@code Log} {@linkplain Block blocks} sorted by {@linkplain Block#getAddress() address}.
     *
     * @return {@linkplain Block blocks} sorted by {@linkplain Block#getAddress() address}
     * @see Block
     * @see Block#getAddress()
     */
    @NotNull
    Iterable<Block> getBlocks();

    /**
     * Returns {@linkplain Block blocks} sorted by {@linkplain Block#getAddress() address} with address greater than
     * or equal to specified {@code fromAddress}.
     *
     * @param fromAddress starting block address
     * @return {@linkplain Block blocks} sorted by {@linkplain Block#getAddress() address}
     * @see Block
     * @see Block#getAddress()
     */
    @NotNull
    Iterable<Block> getBlocks(long fromAddress);

    /**
     * Closes {@code DataReader} and all open resources associated with it (files, connections, etc.). After the
     * {@code DataReader} is closed, any {@linkplain Block block} got using {@linkplain #getBlocks()} or
     * {@linkplain #getBlocks(long)} method would be no longer accessible.
     *
     * @see Block
     * @see #getBlocks()
     * @see #getBlocks(long)
     */
    void close();
}
