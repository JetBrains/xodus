/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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

import java.util.concurrent.atomic.AtomicInteger;

public class BoundedMTProgressTrackerFactory {
    private final double[] progressPerThread;
    private final AtomicInteger progressReporterIndex = new AtomicInteger(-1);

    private final ProgressTracker progressTracker;

    public BoundedMTProgressTrackerFactory(int numThreads, @NotNull ProgressTracker progressTracker) {
        this.progressPerThread = new double[numThreads];
        this.progressTracker = progressTracker;
    }

    public ThreadLocalProgressTracker createThreadLocalTracker(int threadIndex) {
        return new ThreadLocalProgressTracker(threadIndex);
    }

    public final class ThreadLocalProgressTracker implements ProgressTracker, AutoCloseable {
        private final int threadIndex;

        private ThreadLocalProgressTracker(int threadIndex) {
            this.threadIndex = threadIndex;
        }

        @Override
        public void start(String indexName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pushPhase(String phaseName, String... parameters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void progress(double progress) {
            while (progressReporterIndex.get() == -1) {
                if (progressReporterIndex.compareAndSet(-1, threadIndex)) {
                    break;
                }
            }

            progressPerThread[threadIndex] = progress;

            if (progressReporterIndex.get() == threadIndex && progressTracker.isProgressUpdatedRequired()) {
                double totalProgress = 0;
                for (double p : progressPerThread) {
                    totalProgress += p;
                }

                progressTracker.progress(Math.min(100, Math.max(0, totalProgress / progressPerThread.length)));
            }
        }

        @Override
        public boolean isProgressUpdatedRequired() {
            return true;
        }

        @Override
        public void finish() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pullPhase() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            progressPerThread[threadIndex] = 100;
            progressReporterIndex.compareAndSet(threadIndex, -1);
        }
    }
}
