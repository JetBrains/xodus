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
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared timer runs registered periodic tasks (each second) in lock-free manner.
 */
public class SharedTimer {

    private static int PERIOD = 1000; // in milliseconds
    private static final JobProcessor processor;
    private static final AtomicReference<PersistentHashSet<ExpirablePeriodicTask>> registeredTasks;

    static {
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared timer thread");
        registeredTasks = new AtomicReference<>(new PersistentHashSet<ExpirablePeriodicTask>());
        processor.queueIn(new Ticker(), PERIOD);
    }

    private SharedTimer() {
    }

    public static void registerPeriodicTask(@NotNull final ExpirablePeriodicTask task) {
        optimisticUpdateOfTasks(new TasksUpdater() {
            @Override
            public void update(@NotNull final PersistentHashSet.MutablePersistentHashSet<ExpirablePeriodicTask> mutableTasks) {
                mutableTasks.add(task);
            }
        });
    }

    public static void unregisterPeriodicTask(@NotNull final ExpirablePeriodicTask task) {
        optimisticUpdateOfTasks(new TasksUpdater() {
            @Override
            public void update(@NotNull final PersistentHashSet.MutablePersistentHashSet<ExpirablePeriodicTask> mutableTasks) {
                mutableTasks.remove(task);
            }
        });
    }

    private static void optimisticUpdateOfTasks(@NotNull final TasksUpdater updater) {
        for (; ; ) {
            final PersistentHashSet<ExpirablePeriodicTask> current = registeredTasks.get();
            final PersistentHashSet<ExpirablePeriodicTask> copy = current.getClone();
            final PersistentHashSet.MutablePersistentHashSet<ExpirablePeriodicTask> mutableTasks = copy.beginWrite();
            updater.update(mutableTasks);
            mutableTasks.endWrite();
            if (registeredTasks.compareAndSet(current, copy)) {
                break;
            }
        }
    }

    public interface ExpirablePeriodicTask extends Runnable {

        boolean isExpired();
    }

    private interface TasksUpdater {

        void update(@NotNull final PersistentHashSet.MutablePersistentHashSet<ExpirablePeriodicTask> mutableTasks);
    }

    private static class Ticker extends Job {

        @Override
        protected void execute() throws Throwable {
            final long nextTick = System.currentTimeMillis() + PERIOD;
            final Collection<ExpirablePeriodicTask> expiredTasks = new QueueDecorator<>();
            try {
                for (final ExpirablePeriodicTask task : registeredTasks.get()) {
                    if (task.isExpired()) {
                        expiredTasks.add(task);
                    } else {
                        task.run();
                    }
                }
                if (!expiredTasks.isEmpty()) {
                    optimisticUpdateOfTasks(new TasksUpdater() {
                        @Override
                        public void update(@NotNull final PersistentHashSet.MutablePersistentHashSet<ExpirablePeriodicTask> mutableTasks) {
                            for (final ExpirablePeriodicTask expiredTask : expiredTasks) {
                                mutableTasks.remove(expiredTask);
                            }
                        }
                    });
                }
            } finally {
                processor.queueAt(this, nextTick);
            }
        }
    }
}