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
package jetbrains.exodus.io;

import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ServiceLoader;

// TODO: document
public abstract class DataReaderWriterProvider {

    public abstract Pair<DataReader, DataWriter> newReaderWriter(@NotNull final String location);

    @Nullable
    public static DataReaderWriterProvider getProvider(@NotNull final String fqName) {
        for (DataReaderWriterProvider provider : ServiceLoader.load(DataReaderWriterProvider.class)) {
            if (provider.getClass().getCanonicalName().equalsIgnoreCase(fqName)) {
                return provider;
            }
        }
        return null;
    }
}
