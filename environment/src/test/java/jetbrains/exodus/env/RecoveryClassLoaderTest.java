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
package jetbrains.exodus.env;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public class RecoveryClassLoaderTest {

    private static Object env = null;
    private static LogConfig cfg = null;

    static {
        runIsolated(new Runnable() {
            @Override
            public void run() {
            }
        });
    }

    private static final File[] testsDirectory = new File[1];

    @Before
    public void setUp() throws IOException {
        testsDirectory[0] = getEnvDirectory();
        if (testsDirectory[0].exists()) {
            IOUtil.deleteRecursively(testsDirectory[0]);
        } else if (!testsDirectory[0].mkdir()) {
            throw new IOException("Failed to create directory for tests.");
        }
    }

    @After
    public void tearDown() {
        IOUtil.deleteRecursively(testsDirectory[0]);
        IOUtil.deleteFile(testsDirectory[0]);
        testsDirectory[0] = null;
    }

    @Test
    @Ignore
    public void testRecovery() {
        runIsolated(OPEN_ENVIRONMENT);

        runIsolated(BREAK_ENVIRONMENT);

        runIsolated(OPEN_ENVIRONMENT);

        runIsolated(CHECK_ENVIRONMENT);
    }

    private static final Runnable OPEN_ENVIRONMENT = new Runnable() {
        @Override
        public void run() {
            env = Environments.newInstance(cfg = LogConfig.create(new FileDataReader(testsDirectory[0], 16), new FileDataWriter(testsDirectory[0])));
        }
    };

    private static final Runnable BREAK_ENVIRONMENT = new Runnable() {
        @Override
        public void run() {
            final Environment env = (Environment) RecoveryClassLoaderTest.env;
            final LogConfig cfg = RecoveryClassLoaderTest.cfg;

            env.executeInTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull Transaction txn) {
                    env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
                }
            });

            // assertLoggableTypes(log.getLoggablesIterator(0), SEQ);
            env.close();

            final long size = cfg.getReader().getBlock(0).length();
            cfg.getWriter().openOrCreateBlock(0, size - 5);
            cfg.getWriter().close();
        }
    };

    private static final Runnable CHECK_ENVIRONMENT = new Runnable() {
        @Override
        public void run() {
            EnvironmentImpl env = (EnvironmentImpl) RecoveryClassLoaderTest.env;
            Log log = env.getLog();

            // only 'max' first loggables should remain
            // assertLoggableTypes(max, log.getLoggablesIterator(0), SEQ);

            final Iterator<RandomAccessLoggable> iter = log.getLoggableIterator(0);
            Loggable last = null;
            while (iter.hasNext()) {
                last = iter.next();
            }

            Assert.assertNotNull(last);

            Assert.assertEquals(log.getHighAddress(), last.getAddress() + last.length());
        }
    };

    private static void runIsolated(@NotNull final Runnable runnable) {
        final Thread thread = Thread.currentThread();
        final ClassLoader loader = thread.getContextClassLoader();
        final String classPath = System.getProperty("java.class.path");
        final StringTokenizer tokenizer = new StringTokenizer(classPath, ":", false);
        try {
            final ArrayList<URL> tokens = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                tokens.add(new File(tokenizer.nextToken()).toURI().toURL());
            }
            thread.setContextClassLoader(
                    new URLClassLoader(
                            tokens.toArray(new URL[tokens.size()]),
                            loader.getParent()
                    ) {
                        @Override
                        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                            // First, check if the class has already been loaded
                            Class<?> c = findLoadedClass(name);
                            if (c == null) {
                                c = findClass(name);
                            }
                            if (resolve) {
                                resolveClass(c);
                            }
                            return c;
                        }
                    }
            );
            runnable.run();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    protected static File getEnvDirectory() {
        return TestUtil.createTempDir();
    }
}
