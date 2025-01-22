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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public final class EntityStoreSharedAsyncProcessor extends MultiThreadDelegatingJobProcessor {

    private static final String THREAD_NAME = EntityStoreSharedAsyncProcessor.class.getSimpleName();
    private static final Logger logger = LoggerFactory.getLogger(EntityStoreSharedAsyncProcessor.class);
    private static final JobProcessorExceptionHandler EXCEPTION_HANDLER = new EntityStoreSharedAsyncProcessorExceptionHandler();

    private volatile Function<Job, Void> beforeJobHandler = null;
    private volatile Function<Job, Void> afterJobHandler = null;


    public EntityStoreSharedAsyncProcessor(final String threadName, final int threadCount) {
        super(threadName, threadCount);
        setExceptionHandler(EXCEPTION_HANDLER);
    }

    public EntityStoreSharedAsyncProcessor(final int threadCount) {
        super(THREAD_NAME, threadCount);
        setExceptionHandler(EXCEPTION_HANDLER);
    }

    private static final class EntityStoreSharedAsyncProcessorExceptionHandler implements JobProcessorExceptionHandler {

        @Override
        public void handle(final JobProcessor processor, final Job job, final Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @SuppressWarnings("unused")
    public void setBeforeJobHandler(Function<Job, Void> beforeJobHandler) {
        this.beforeJobHandler = beforeJobHandler;
    }

    @SuppressWarnings("unused")
    public void setAfterJobHandler(Function<Job, Void> afterJobHandler) {
        this.afterJobHandler = afterJobHandler;
    }

    @Override
    public void beforeProcessingJob(@NotNull Job job) {
        super.beforeProcessingJob(job);
        if (beforeJobHandler != null) {
            beforeJobHandler.apply(job);
        }
    }

    @Override
    public void afterProcessingJob(@NotNull Job job) {
        super.afterProcessingJob(job);
        if (afterJobHandler != null) {
            afterJobHandler.apply(job);
        }
    }
}
