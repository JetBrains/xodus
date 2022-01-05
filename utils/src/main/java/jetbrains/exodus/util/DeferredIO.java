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
package jetbrains.exodus.util;

import jetbrains.exodus.core.execution.JobProcessorAdapter;
import jetbrains.exodus.core.execution.ThreadJobProcessorPool;

public class DeferredIO {

    private static volatile JobProcessorAdapter deferredIOProcessor = null;

    private DeferredIO() {
    }

    public static JobProcessorAdapter getJobProcessor() {
        if (deferredIOProcessor == null) {
            synchronized (DeferredIO.class) {
                if (deferredIOProcessor == null) {
                    deferredIOProcessor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared deferred I/O job processor");
                }
            }
        }
        return deferredIOProcessor;
    }
}
