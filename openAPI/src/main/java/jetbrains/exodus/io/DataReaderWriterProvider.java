/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ServiceLoader;

/**
 * Service provider interface for creation instances of {@linkplain DataReader} and {@linkplain DataWriter}.
 * {@linkplain DataReader} and {@linkplain DataWriter} are used by {@code Log} implementation to perform basic
 * operations with {@linkplain Block blocks} ({@code .xd} files) and basic read/write/delete operations.
 *
 * Service provider interface is identified by a fully-qualified name of its implementation. When opening an
 * {@linkplain Environment}, {@linkplain #DEFAULT_READER_WRITER_PROVIDER} is used as default provide name. To use a
 * custom I/O provider, specify its fully-qualified name as a parameter of {@linkplain EnvironmentConfig#setLogDataReaderWriterProvider}.
 *
 * On {@linkplain Environment} creation new instance of {@code DataReaderWriterProvider} is created.
 *
 * @see Block
 * @see DataReader
 * @see DataWriter
 * @see EnvironmentConfig#getLogDataReaderWriterProvider
 * @see EnvironmentConfig#setLogDataReaderWriterProvider
 * @since 1.3.0
 */
public abstract class DataReaderWriterProvider {

    /**
     * Fully-qualified name of default {@code DataReaderWriteProvider}.
     */
    public static final String DEFAULT_READER_WRITER_PROVIDER = "jetbrains.exodus.io.FileDataReaderWriterProvider";

    /**
     * Fully-qualified name of read-only watching {@code DataReaderWriteProvider}.
     */
    public static final String WATCHING_READER_WRITER_PROVIDER = "jetbrains.exodus.io.WatchingFileDataReaderWriterProvider";

    /**
     * Fully-qualified name of in-memory {@code DataReaderWriteProvider}.
     */
    public static final String IN_MEMORY_READER_WRITER_PROVIDER = "jetbrains.exodus.io.MemoryDataReaderWriterProvider";


    /**
     * Creates pair of new instances of {@linkplain DataReader} and {@linkplain DataWriter} by specified location.
     * What is location depends on the implementation of {@code DataReaderWriterProvider}, e.g. for {@code FileDataReaderWriterProvider}
     * location is a full path on local file system where the database is located.
     *
     * @param location identifies the database in this {@code DataReaderWriterProvider}
     * @return pair of new instances of {@linkplain DataReader} and {@linkplain DataWriter}
     */
    public abstract Pair<DataReader, DataWriter> newReaderWriter(@NotNull final String location);

    /**
     * Returns {@code true} if the {@code DataReaderWriterProvider} creates in-memory {@linkplain DataReader} and {@linkplain DataWriter}.
     *
     * @return {@code true} if the {@code DataReaderWriterProvider} creates in-memory {@linkplain DataReader} and {@linkplain DataWriter}
     */
    public boolean isInMemory() {
        return false;
    }

    /**
     * Returns {@code true} if the {@code DataReaderWriterProvider} creates read-only {@linkplain DataWriter}.
     *
     * @return {@code true} if the {@code DataReaderWriterProvider} creates read-only {@linkplain DataWriter}
     */
    public boolean isReadonly() {
        return false;
    }

    /**
     * Callback method which is called when an environment is been opened/created. Can be used in implementation of
     * the {@code DataReaderWriterProvider} to access directly an {@linkplain Environment} instance,
     * its {@linkplain Environment#getEnvironmentConfig() config}, etc. Creation of {@code environment} is not
     * completed when the method is called.
     *
     * @param environment {@linkplain Environment} instance which is been opened/created using this
     *                    {@code DataReaderWriterProvider}
     */
    public void onEnvironmentCreated(@NotNull final Environment environment) {
    }

    /**
     * Gets a {@code DataReaderWriterProvider} implementation by specified provider name.
     *
     * @param providerName fully-qualified name of {@code DataReaderWriterProvider} implementation
     * @return {@code DataReaderWriterProvider} implementation or {@code null} if the service could not be loaded
     */
    @Nullable
    public static DataReaderWriterProvider getProvider(@NotNull final String providerName) {
        ServiceLoader<DataReaderWriterProvider> serviceLoader = ServiceLoader.load(DataReaderWriterProvider.class);
        if (!serviceLoader.iterator().hasNext()) {
            serviceLoader = ServiceLoader.load(DataReaderWriterProvider.class, DataReaderWriterProvider.class.getClassLoader());
        }
        for (DataReaderWriterProvider provider : serviceLoader) {
            if (provider.getClass().getCanonicalName().equalsIgnoreCase(providerName)) {
                return provider;
            }
        }
        return null;
    }
}
