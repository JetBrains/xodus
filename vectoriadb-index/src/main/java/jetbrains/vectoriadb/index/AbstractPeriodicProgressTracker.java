/*
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
package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractPeriodicProgressTracker implements ProgressTracker {
    private final Timer timer = new Timer("ProgressTracker", true);
    private final long period;
    private volatile boolean timerStarted = false;

    private volatile String indexName;

    private final ReentrantLock timerLock = new ReentrantLock();
    private final AtomicBoolean requestProgressUpdate = new AtomicBoolean(true);
    protected final ConcurrentLinkedDeque<ProgressPhase> phases = new ConcurrentLinkedDeque<>();

    public AbstractPeriodicProgressTracker(int period) {
        this.period = TimeUnit.SECONDS.toMillis(period);
    }

    @Override
    public void start(String indexName) {
        this.indexName = indexName;
        assert phases.isEmpty();
    }

    @Override
    public void finish() {
        timer.cancel();
        this.indexName = null;

        assert phases.isEmpty();
    }

    @Override
    public void pushPhase(String phaseName, String... parameters) {
        var phase = new ProgressPhase(indexName, phaseName, parameters);
        phases.addLast(phase);
        reportProgress();
    }

    @Override
    public void progress(double progress) {
        scheduleProgressTimer();

        var phase = phases.peekLast();
        assert phase != null;

        phase.progress = Math.max(phase.progress, Math.min(100, Math.max(0, progress)));

        if (requestProgressUpdate.get()) {
            requestProgressUpdate.compareAndSet(true, false);
            reportProgress();
        }
    }

    protected abstract void reportProgress();

    @NotNull
    String createConsoleOutput() {
        if (indexName == null || phases.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        builder.append(indexName).append(" : ");

        int counter = 0;
        for (var phase : phases) {
            if (counter > 0) {
                builder.append(" -> ");
            }

            builder.append(phase.phaseName);
            var parameters = phase.parameters;

            if (parameters.length > 0) {
                builder.append(" ");
            }

            for (int j = 0; j < parameters.length; j += 2) {
                builder.append("{");
                builder.append(parameters[j]);
                builder.append(":");
                builder.append(parameters[j + 1]);
                builder.append("}");

                if (j < parameters.length - 2) {
                    builder.append(", ");
                }
            }
            if (phase.progress >= 0) {
                builder.append(" [").append(String.format("%.2f", phase.progress)).append("%]");
            }
            counter++;
        }
        return builder.toString();
    }

    private void scheduleProgressTimer() {
        if (!timerStarted) {
            timerLock.lock();
            try {
                if (timerStarted) {
                    return;
                }

                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        requestProgressUpdate.set(true);
                    }
                }, period, period);

                timerStarted = true;
            } finally {
                timerLock.unlock();
            }
        }
    }

    @Override
    public void pullPhase() {
        phases.removeLast();
    }

    @Override
    public boolean isProgressUpdatedRequired() {
        return requestProgressUpdate.get();
    }

    protected static final class ProgressPhase {
        public final String indexName;

        public double progress = -1;
        public final String phaseName;
        public final String[] parameters;

        public ProgressPhase(String indexName, String phaseName, String... parameters) {
            this.indexName = indexName;
            this.phaseName = phaseName;
            this.parameters = parameters;
        }
    }
}
