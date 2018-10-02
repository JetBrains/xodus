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
package jetbrains.exodus.core.execution;

import jetbrains.exodus.core.dataStructures.decorators.QueueDecorator;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Set;

/**
 * Shared timer runs registered periodic tasks (each second) in lock-free manner.
 */
public class SharedTimer {

    private static int PERIOD = 1000; // in milliseconds
    private static final Set<ExpirablePeriodicTask> registeredTasks;
    private static final JobProcessor processor;

    static {
        registeredTasks = new HashSet<>();
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared timer thread");
        processor.queueIn(new Ticker(), PERIOD);
    }

    private SharedTimer() {
    }

    public static void registerPeriodicTask(@NotNull final ExpirablePeriodicTask task) {
        processor.queue(new Job() {
            @Override
            protected void execute() {
                registeredTasks.add(task);
            }
        });
    }

    public static void unregisterPeriodicTask(@NotNull final ExpirablePeriodicTask task) {
        processor.queue(new Job() {
            @Override
            protected void execute() {
                registeredTasks.remove(task);
            }
        });
    }

    public interface ExpirablePeriodicTask extends Runnable {

        boolean isExpired();
    }

    private static class Ticker extends Job {

        @Override
        protected void execute() {
            final long nextTick = System.currentTimeMillis() + PERIOD;
            final Collection<ExpirablePeriodicTask> expiredTasks = new QueueDecorator<>();
            try {
                for (final ExpirablePeriodicTask task : registeredTasks) {
                    if (task.isExpired()) {
                        expiredTasks.add(task);
                    } else {
                        task.run();
                    }
                }
                if (!expiredTasks.isEmpty()) {
                    for (final ExpirablePeriodicTask expiredTask : expiredTasks) {
                        registeredTasks.remove(expiredTask);
                    }
                }
            } finally {
                processor.queueAt(this, nextTick);
            }
        }
    }
}