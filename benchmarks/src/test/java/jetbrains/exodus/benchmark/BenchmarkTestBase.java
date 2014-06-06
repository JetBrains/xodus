/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.StringKVNode;
import jetbrains.exodus.util.Random;
import jetbrains.exodus.util.TeamCityMessenger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public abstract class BenchmarkTestBase {

    private static final String TEAMCITY_MESSAGES = "teamcity.messages";

    protected static final DecimalFormat FORMATTER;
    protected static final Random RANDOM;

    static {
        FORMATTER = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMATTER.applyPattern("00000");
        RANDOM = new Random();
    }

    protected TeamCityMessenger myMessenger = null;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void start() {
        final String userHome = System.getProperty("user.home");
        if (userHome == null || userHome.length() == 0) {
            throw new ExodusException("user.home is undefined.");
        }
        String tcMsgFileName = System.getProperty(TEAMCITY_MESSAGES);
        if (tcMsgFileName != null) {
            try {
                myMessenger = new TeamCityMessenger(tcMsgFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public void end() throws IOException {
        if (myMessenger != null) {
            myMessenger.close();
        }
    }

    public static void time(String title, Runnable code, int iterations) {
        long t = System.nanoTime();
        code.run();
        long t2 = System.nanoTime();
        long iterAvg = (t2 - t) / iterations;
        System.out.println(title);
        System.out.println("Capacity: " + (long) (1.0E9 / iterAvg) + " per second");
        System.out.println("Average:  " + iterAvg + " ns");
    }

    public static long time(String title, Runnable code) {
        long t = System.currentTimeMillis();
        code.run();
        long t2 = System.currentTimeMillis();
        long time = t2 - t;
        System.out.println(title + ((double) time / 1000.0f) + 's');
        return time;
    }

    protected void message(String key, long value) {
        if (myMessenger != null) {
            myMessenger.putValue(key, value);
        }
    }

    public static INode kv(String key) {
        return new StringKVNode(key, "");
    }

    public static INode kv(String key, String value) {
        return new StringKVNode(key, value);
    }

    public static INode kv(int key, String value) {
        return kv(FORMATTER.format(key), value);
    }

    public static ByteIterable v(int value) {
        return value("val " + FORMATTER.format(value));
    }

    public static ArrayByteIterable key(String key) {
        return key == null ? null : new ArrayByteIterable(key.getBytes());
    }

    public static ByteIterable value(String value) {
        return key(value);
    }

    public static ArrayByteIterable key(int key) {
        return key(FORMATTER.format(key));
    }

    public static ArrayByteIterable key(long key) {
        return LongBinding.longToEntry(key);
    }

    public static ByteIterable value(long value) {
        return key(value);
    }


}
