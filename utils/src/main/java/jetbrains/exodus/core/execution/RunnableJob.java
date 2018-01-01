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

public class RunnableJob extends Job {

    private Runnable runnable;

    public RunnableJob(Runnable runnable) {
        this.runnable = runnable;
    }

    public RunnableJob(@NotNull final JobProcessor processor, Runnable runnable) {
        this(processor, Priority.normal, runnable);
    }

    public RunnableJob(@NotNull JobProcessor processor, @NotNull Priority priority, Runnable runnable) {
        setProcessor(processor);
        this.runnable = runnable;
        queue(priority);
    }

    @Override
    protected void execute() throws Throwable {
        runnable.run();
    }

    @Override
    public String getName() {
        final String name = runnable.getClass().getSimpleName();
        return name.isEmpty() ? "Anonymous runnable job" : "Runnable job " + name;
    }

    @Override
    public String getGroup() {
        return runnable.getClass().getName();
    }
}
