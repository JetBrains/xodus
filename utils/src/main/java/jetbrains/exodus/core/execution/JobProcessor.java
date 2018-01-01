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

import jetbrains.exodus.core.dataStructures.Priority;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface JobProcessor {

    void start();

    void finish();

    boolean isFinished();

    void setExceptionHandler(@Nullable JobProcessorExceptionHandler handler);

    @Nullable
    JobProcessorExceptionHandler getExceptionHandler();

    boolean queue(Job job);

    boolean queue(Job job, Priority priority);

    Job queueAt(Job job, long millis);

    Job queueIn(Job job, long millis);

    int pendingJobs();

    int pendingTimedJobs();

    void waitForJobs(final long spinTimeout);

    void waitForTimedJobs(final long spinTimeout);

    void suspend() throws InterruptedException;

    void resume() throws InterruptedException;

    boolean isSuspended();

    void beforeProcessingJob(@NotNull final Job job);

    void afterProcessingJob(@NotNull final Job job);

    @Nullable
    Job getCurrentJob();

    long getCurrentJobStartedAt();

    @NotNull
    Iterable<Job> getPendingJobs();
}
