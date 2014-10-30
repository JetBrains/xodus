/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.ThreadJobProcessorPool;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

final class StuckTransactionMonitor extends Job {

    private static final org.apache.commons.logging.Log logging = LogFactory.getLog(StuckTransactionMonitor.class);

    @NotNull
    private final EnvironmentImpl env;
    @NotNull
    private final JobProcessor processor;

    StuckTransactionMonitor(@NotNull final EnvironmentImpl env) {
        this.env = env;
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared stuck transaction monitor");
        queueThis();
    }

    @Override
    protected void execute() throws Throwable {
        if (env.isOpen()) {
            try {
                final TransactionImpl oldestTxn = env.getOldestTransaction();
                if (oldestTxn == null) {
                    return;
                }
                final long created = oldestTxn.getCreated();
                if (created + env.getEnvironmentConfig().getEnvMonitorTxnsTimeout() < System.currentTimeMillis()) {
                    final Thread creatingThread = oldestTxn.getCreatingThread();
                    logging.error("Transaction timed out: created at " + new Date(created).toString() + ", thread = " +
                            creatingThread + '(' + (creatingThread == null ? "" : creatingThread.getId()) + ')', oldestTxn.getTrace());
                }
            } finally {
                queueThis();
            }
        }
    }

    private void queueThis() {
        processor.queueIn(this, env.getEnvironmentConfig().getEnvMonitorTxnsCheckFreq());
    }
}
