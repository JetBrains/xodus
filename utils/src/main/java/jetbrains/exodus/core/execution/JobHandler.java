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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"AbstractClassNeverImplemented"})
public abstract class JobHandler {

    public abstract void handle(final Job job);

    @NotNull
    static JobHandler[] append(@Nullable final JobHandler[] handlers, @NotNull final JobHandler handle) {
        final int size = (handlers == null) ? 0 : handlers.length;
        final JobHandler[] result = new JobHandler[size + 1];
        if (size > 0) {
            System.arraycopy(handlers, 0, result, 0, size);
        }
        result[size] = handle;
        return result;
    }

    static JobHandler[] remove(@Nullable final JobHandler[] handlers, JobHandler handle) {
        final int size = (handlers == null) ? 0 : handlers.length;
        if (size <= 1) {
            return null;
        } else {
            final JobHandler[] result = new JobHandler[size - 1];
            int i = 0;
            while (i < size) {
                final JobHandler handler = handlers[i];
                if (handler == handle) {
                    handle = null;
                    ++i;
                } else {
                    result[i] = handle;
                }
                ++i;
            }
            return result;
        }
    }

    static void invokeHandlers(@Nullable final JobHandler[] handlers, @NotNull final Job job) {
        if (handlers != null) {
            for (final JobHandler handler : handlers) {
                handler.handle(job);
            }
        }
    }
}
