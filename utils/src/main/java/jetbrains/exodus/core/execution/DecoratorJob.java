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

public abstract class DecoratorJob extends Job {

    @NotNull
    private Job decorated;

    protected DecoratorJob() {
        decorated = this;
    }

    protected DecoratorJob(@NotNull final JobProcessor processor,
                           @NotNull final Job decorated) {
        this(processor, Priority.normal, decorated);
    }

    protected DecoratorJob(@NotNull final JobProcessor processor,
                           @NotNull final Priority priority,
                           @NotNull final Job decorated) {
        super();
        this.decorated = decorated;
        setProcessor(processor);
        queue(priority);
    }

    /**
     * Factory method to decorate a job for execution with normal priority.
     *
     * @param decorated source job.
     * @return decorator job.
     */
    public abstract DecoratorJob newDecoratorJob(final Job decorated);

    /**
     * Factory method to decorate a job for execution with specified priority.
     *
     * @param decorated source job.
     * @param priority  priority which to execute the job with.
     * @return decorator job.
     */
    public abstract DecoratorJob newDecoratorJob(final Job decorated, final Priority priority);

    @NotNull
    public Job getDecorated() {
        return decorated;
    }

    public void setDecorated(@NotNull final Job decorated) {
        this.decorated = decorated;
        setProcessor(decorated.getProcessor());
    }

    protected void executeDecorated() throws Throwable {
        decorated.execute();
    }

    @Override
    public String getName() {
        return decorated.getName();
    }

    @Override
    public String getGroup() {
        return decorated.getGroup();
    }

    public int hashCode() {
        return decorated.hashCode();
    }

    @Override
    public boolean isEqualTo(Job job) {
        return decorated.equals(((DecoratorJob) job).decorated);
    }
}
