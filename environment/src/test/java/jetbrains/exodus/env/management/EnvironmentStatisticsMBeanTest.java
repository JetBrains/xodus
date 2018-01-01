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
package jetbrains.exodus.env.management;

import jetbrains.exodus.env.EnvironmentTestsBase;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

public class EnvironmentStatisticsMBeanTest extends EnvironmentTestsBase {

    private static final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

    @Test
    public void beanIsAccessible() throws MalformedObjectNameException {
        final Set<ObjectInstance> envConfigInstances =
                platformMBeanServer.queryMBeans(new ObjectName(EnvironmentStatistics.getObjectName(env)), null);
        Assert.assertNotNull(envConfigInstances);
        Assert.assertFalse(envConfigInstances.isEmpty());
    }
}

