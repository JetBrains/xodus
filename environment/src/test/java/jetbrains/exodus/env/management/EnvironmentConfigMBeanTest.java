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

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.env.EnvironmentTestsBase;
import jetbrains.exodus.env.ReadonlyTransactionException;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import org.junit.Assert;
import org.junit.Test;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Set;

public class EnvironmentConfigMBeanTest extends EnvironmentTestsBase {

    private static final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    private static final String READ_ONLY_ATTR = "EnvIsReadonly";

    private ObjectName envConfigName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        envConfigName = null;
        envConfigName = new ObjectName(EnvironmentConfig.getObjectName(env));
    }

    @Test
    public void beanIsAccessible() throws MalformedObjectNameException {
        Assert.assertNotNull(envConfigName);
        final Set<ObjectInstance> envConfigInstances =
                platformMBeanServer.queryMBeans(new ObjectName(EnvironmentConfig.getObjectName(env)), null);
        Assert.assertNotNull(envConfigInstances);
        Assert.assertFalse(envConfigInstances.isEmpty());
    }

    @Test
    public void readOnly() throws Exception {
        beanIsAccessible();
        Assert.assertFalse((Boolean) platformMBeanServer.getAttribute(envConfigName, READ_ONLY_ATTR));
        platformMBeanServer.setAttribute(envConfigName, new Attribute(READ_ONLY_ATTR, true));
        Assert.assertTrue((Boolean) platformMBeanServer.getAttribute(envConfigName, READ_ONLY_ATTR));
        Assert.assertTrue(env.getEnvironmentConfig().getEnvIsReadonly());
        platformMBeanServer.setAttribute(envConfigName, new Attribute(READ_ONLY_ATTR, false));
    }

    @Test
    public void readOnly_XD_444() throws Exception {
        beanIsAccessible();
        final Transaction txn = env.beginTransaction();
        try {
            env.openStore("New Store", StoreConfig.WITHOUT_DUPLICATES, txn);
            Assert.assertFalse(txn.isIdempotent());
            platformMBeanServer.setAttribute(envConfigName, new Attribute(READ_ONLY_ATTR, true));
            TestUtil.runWithExpectedException(new Runnable() {
                @Override
                public void run() {
                    txn.flush();
                }
            }, ReadonlyTransactionException.class);
        } finally {
            txn.abort();
        }
    }

    @Test
    public void readOnly_XD_448() throws Exception {
        beanIsAccessible();
        platformMBeanServer.setAttribute(envConfigName, new Attribute(READ_ONLY_ATTR, true));
        platformMBeanServer.setAttribute(envConfigName, new Attribute(READ_ONLY_ATTR, true));
    }
}
