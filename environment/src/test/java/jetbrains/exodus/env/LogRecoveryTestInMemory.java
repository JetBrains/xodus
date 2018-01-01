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

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.inMemory.Memory;
import jetbrains.exodus.io.inMemory.MemoryDataReader;
import jetbrains.exodus.io.inMemory.MemoryDataWriter;

public class LogRecoveryTestInMemory extends LogRecoveryTest {

    @Override
    protected Pair<DataReader, DataWriter> createRW() {
        Memory memory = new Memory();
        return new Pair<DataReader, DataWriter>(new MemoryDataReader(memory), new MemoryDataWriter(memory));
    }

    @Override
    protected void deleteRW() {
        reader = null;
        writer = null;
    }
}
