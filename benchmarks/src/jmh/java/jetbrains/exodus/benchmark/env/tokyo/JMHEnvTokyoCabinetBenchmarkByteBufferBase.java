/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.benchmark.env.tokyo;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import jetbrains.exodus.benchmark.env.JMHEnvBenchmarkBase;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class JMHEnvTokyoCabinetBenchmarkByteBufferBase extends JMHEnvBenchmarkBase {
    static final ByteBuffer[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveEntriesByteBuffer(TokyoCabinetBenchmark.KEYS_COUNT);
    static final ByteBuffer[] randomKeys = TokyoCabinetBenchmark.getRandomEntriesByteBuffer(TokyoCabinetBenchmark.KEYS_COUNT);

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        super.setup();
    }

    void writeSuccessiveKeys() {
        env.executeInTransaction(txn -> {
            for (final ByteBuffer key : successiveKeys) {
                store.add(txn, key, key);
            }
        });
    }
}
