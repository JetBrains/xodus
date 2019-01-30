/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.io;

import jetbrains.exodus.env.EnvironmentConfig;

/**
 * {@code Block} represents single {@code .xd} file in {@code Log} on storage device - disk, memory, network file
 * system. etc. {@code Block} is identified in the log by its {@linkplain #getAddress() address}. {@code Block}
 * has {@linkplain #length() length} in bytes, which is not greater than {@linkplain EnvironmentConfig#getLogFileSize()
 * maximum log block size}. {@code Block} implementation defines the way data is
 * {@linkplain #read(byte[], long, int, int) read} from storage device.
 *
 * {@code Log} blocks can be mutable and immutable. All blocks having length equal to {@linkplain EnvironmentConfig#getLogFileSize()
 * maximum log block size} are immutable. In any moment, only one block can be mutable. This has maximum {@linkplain
 * #getAddress() address}.
 *
 * @see DataReader
 * @see DataWriter
 * @see DataReaderWriterProvider
 * @see EnvironmentConfig#getLogFileSize()
 * @since 1.3.0
 */
public interface Block {

    /**
     * Returns address of the block in the {@code Log}. This address is always constant for the block.
     *
     * @return address of the block in the {@code Log}
     */
    long getAddress();

    /**
     * Returns length of the block in bytes. Block length is always not greater
     * than {@linkplain EnvironmentConfig#getLogFileSize() maximum log file size}.
     *
     * @return length of the block in bytes
     * @see EnvironmentConfig#getLogFileSize()
     */
    long length();

    /**
     * Reads data from the underlying store device.
     *
     * @param output   array to copy data
     * @param position starting position in the {@code .xd} file
     * @param offset   starting offset in the array
     * @param count    number of bytes to read
     * @return actual number of bytes read
     */
    int read(byte[] output, long position, int offset, int count);

    /**
     * For immutable {@code Block} implementations, this method returns fresh representation of the same {@code .xd}
     * file. For mutable {@code Block} implementations, this method just returns this {@code Block} instance.
     *
     * @return fresh representation of the same {@code .xd} file
     */
    Block refresh();
}
