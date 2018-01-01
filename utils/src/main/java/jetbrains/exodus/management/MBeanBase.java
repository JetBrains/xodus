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
package jetbrains.exodus.management;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public abstract class MBeanBase {

    @NotNull
    private final ObjectName name;
    @Nullable
    private Runnable runOnClose;


    protected MBeanBase(@NotNull final String objectName) {
        try {
            this.name = new ObjectName(objectName);
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
        } catch (Exception e) {
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    @NotNull
    public ObjectName getName() {
        return name;
    }

    public void close() {
        final Runnable runOnClose = this.runOnClose;
        if (runOnClose != null) {
            runOnClose.run();
        }
    }

    public void unregister() {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        } catch (InstanceNotFoundException ignore) {
        } catch (Exception e) {
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    public void runOnClose(@Nullable final Runnable runnable) {
        this.runOnClose = runnable;
    }

    protected static String escapeLocation(@NotNull final String location) {
        return location.indexOf(':') >= 0 ? location.replace(':', '@') : location;
    }
}
