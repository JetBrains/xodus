/**
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

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncFileDataReaderWriterProvider extends DataReaderWriterProvider {
    private @Nullable EnvironmentImpl env;

    @Override
    public Pair<DataReader, DataWriter> newReaderWriter(@NotNull String location) {
        var reader = new FileDataReader(WatchingFileDataReaderWriterProvider.checkDirectory(location));
        String lockId = null;

        if (env != null) {
            lockId = env.getEnvironmentConfig().getLogLockId();
        }

        return new Pair<>(reader, new AsyncFileDataWriter(reader, lockId));
    }

    @Override
    public void onEnvironmentCreated(@NotNull Environment environment) {
        this.env = (EnvironmentImpl) environment;
        super.onEnvironmentCreated(environment);
    }
}
