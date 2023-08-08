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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.LongIntPair;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.EnvironmentConfig;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * {@code DataWriter} defines the way how data is written to {@code Log}, how {@linkplain Block blocks} appear in
 * the log and how they are removed from the log.
 * {@code Log} blocks can be mutable and immutable. All blocks having length equal to {@linkplain EnvironmentConfig#getLogFileSize()
 * maximum log block size} are immutable. In any moment, only one block can be mutable. This has maximum {@linkplain
 * Block#getAddress() address}. {@code DataWriter} always writes to a single mutable block, that makes {@code log}
 * appendable.
 *
 * @see Block
 * @see DataReader
 * @see DataReaderWriterProvider
 * @since 1.3.0
 */
@SuppressWarnings("InterfaceWithOnlyOneDirectInheritor")
public interface DataWriter extends Closeable {

    /**
     * Returns {@code true} is the DataWriter is open. i.e. there is an incomplete mutable {@linkplain Block block}
     * having length less than {@linkplain EnvironmentConfig#getLogFileSize() maximum log block size}.
     *
     * @return {@code true} is the DataWriter is open
     */
    boolean isOpen();

    /**
     * Writes (appends) binary data to {@code Log}. Returns new {@linkplain Block} instance representing mutable block.
     *
     * @param b   binary data array
     * @param off starting offset in the array
     * @param len number of byte to write
     * @return mutable {@linkplain Block} instance
     * @see Block
     */
    Block write(byte[] b, int off, int len);

    /**
     * If applicable, forces flush of written data to underlying storage device. Transaction durability depends
     * on the implementation of this method.
     */
    void sync();

    /**
     * If applicable, forces flush of changes in directory structure to underlying storage device.
     */
    void syncDirectory();

    /**
     * Closes the data writer. {@code Log} closes the data writer each time when mutable {@linkplain Block block}
     * becomes immutable, i.e. reaches its {@linkplain EnvironmentConfig#getLogFileSize() maximum log block size}.
     */
    void close();

    /**
     * Clears {@code Log} by location specified as a parameter to {@linkplain DataReaderWriterProvider#newReaderWriter(String)}.
     * The database becomes empty.
     */
    void clear();

    /**
     * Open existing {@linkplain Block block} for writing or creates the new one by specified address with specified length.
     * This method is used in the log recovery procedure, so {@code DataWriter} should truncate existing block
     * if its physical length is greater than specified one. This method is always applied to a mutable {@linkplain Block block}.
     *
     * @param address address of {@linkplain Block block} to open or create
     * @param length  valid {@linkplain Block block} length
     * @return mutable {@linkplain Block} instance
     */
    Block openOrCreateBlock(long address, long length);

    /**
     * Removes existing immutable {@linkplain Block block}. If applicable, this method can rename
     * {@linkplain Block block} (file on a file system) instead of removing it if specified
     * {@code RemoveBlockType rbt} is equal to {@linkplain RemoveBlockType#Rename}.
     *
     * @param blockAddress address of {@linkplain Block block} to remove
     * @param rbt          {@linkplain RemoveBlockType#Rename} to rename {@linkplain Block block} instead of removing it.
     * @see RemoveBlockType
     */
    void removeBlock(long blockAddress, @NotNull RemoveBlockType rbt);

    /**
     * If applicable, tries to acquire writer's lock in specified time. If the lock is acquired, returns {@code true}.
     * Successfully acquired lock guarantees that {@code Log} cannot be opened in parallel (within same
     * JVM or not) unless it is released by the writer.
     *
     * @param timeout - time to wait for lock acquisition
     * @return {@code true} if the lock is acquired
     * @see #release()
     * @see #lockInfo()
     */
    boolean lock(long timeout);

    /**
     * Releases writer's lock.
     *
     * @return {@code true} if released successfully
     * @see #lock(long)
     */
    boolean release();

    /**
     * For debugging purposes, returns detailed information about current lock owner. If {@linkplain #lock(long)}
     * return {@code false}, {@code Log} throws an {@linkplain ExodusException} with the lock info in its message.
     *
     * @return Human-readable information about lock owner
     * @see #lock(long)
     */
    String lockInfo();

    /**
     * Asynchronously writes binary data to {@code Log}.
     * Returns new {@linkplain Block} instance representing mutable block and completable future which will be executed
     * once asynchronous write was executed.
     * This method is not thread safe and can not be used by multiple threads without external synchronization.
     *
     * @param b   binary data array
     * @param off starting offset in the array
     * @param len number of byte to write
     * @return  Pair which consist of mutable {@linkplain Block} instance and completable future which services
     * as indicator of completion (successful or unsuccessful) of requested write.
     * Result of execution of completable future are: start address of the position where data is written and
     * amount of data was written in bytes.
     * @see Block
     * @since 3.0
     */
    Pair<Block, CompletableFuture<LongIntPair>> asyncWrite(byte[] b, int off, int len);


    /**
     * Return current position of writer inside the file.
     * This method is not thread safe and can not be used without external synchronization.
     * @return current position of writer inside the file.
     * @since 3.0
     */
    long position();
}
