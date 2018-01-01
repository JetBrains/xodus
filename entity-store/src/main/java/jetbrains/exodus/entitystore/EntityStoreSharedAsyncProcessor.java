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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EntityStoreSharedAsyncProcessor extends MultiThreadDelegatingJobProcessor {

    private static final String THREAD_NAME = EntityStoreSharedAsyncProcessor.class.getSimpleName();
    private static final Logger logger = LoggerFactory.getLogger(EntityStoreSharedAsyncProcessor.class);
    private static final JobProcessorExceptionHandler EXCEPTION_HANDLER = new EntityStoreSharedAsyncProcessorExceptionHandler();

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
}
