/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree;

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.tree.btree.LeafNodeKV;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public abstract class TreeBaseTest {

    private static final DecimalFormat FORMATTER;
    protected static final Random RANDOM;

    static {
        FORMATTER = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMATTER.applyPattern("00000");
        RANDOM = new Random(77634963005211L);
    }

    protected ITree t;
    protected ITreeMutable tm;
    protected static Log log = null;
    protected static File tempFolder = null;

    public ITree getTree() {
        return t;
    }

    public ITreeMutable getTreeMutable() {
        return tm;
    }

    protected abstract ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId);

    protected abstract ITree openTree(long address, boolean hasDuplicates);

    @Before
    public void start() {
        final String userHome = System.getProperty("user.home");
        if (userHome == null || userHome.length() == 0) {
            throw new ExodusException("user.home is undefined.");
        }
        tempFolder = TestUtil.createTempDir();
        createLog();
    }

    private void createLog() {
        LogConfig config = createLogConfig();
        config.setLocation(tempFolder.getPath());
        log = Environments.newLogInstance(config);
    }

    protected LogConfig createLogConfig() {
        return new LogConfig().setNonBlockingCache(true).setReaderWriterProvider(EnvironmentConfig.DEFAULT.getLogDataReaderWriterProvider());
    }

    @After
    public void end() {
        log.close();
        IOUtil.deleteRecursively(tempFolder);
        IOUtil.deleteFile(tempFolder);
    }

    protected void reopen() {
        log.close();
        createLog();
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

    public static void valueEquals(String expectedValue, ByteIterable ln) {
        Assert.assertEquals(expectedValue, new String(ln.getBytesUnsafe(), 0, ln.getLength()));
    }

    protected static void checkEmptyTree(ITree bt) {
        assertEquals(log, bt.getLog());
        assertTrue(bt.isEmpty());
        assertEquals(0, bt.getSize());
        assertNull(bt.get(ByteIterable.EMPTY));
        assertNull(bt.get(key("some key")));
        assertFalse(bt.openCursor().getNext());
        assertFalse(bt.hasKey(key("some key")));
        assertFalse(bt.hasPair(key("some key"), value("some value")));
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

    public static void assertMatchesIterator(ITree actual, List<INode> expected) {
        assertMatchesIterator(actual, expected.toArray(new INode[expected.size()]));
    }

    public static void assertMatchesIteratorAndExists(ITree actual, INode... expected) {
        assertMatchesIterator(actual, true, expected);
    }

    public static void assertMatchesIterator(ITree actual, INode... expected) {
        assertMatchesIterator(actual, false, expected);
    }

    public static void assertMatchesIterator(ITree actual, boolean checkExists, INode... expected) {
        final ITreeCursor it1 = actual.openCursor();
        List<INode> act = new ArrayList<>((int) actual.getSize());
        while (it1.getNext()) {
            act.add(new LeafNodeKV(it1.getKey(), it1.getValue()));
        }

        Assert.assertArrayEquals(expected, act.toArray(new INode[act.size()]));

        if (checkExists) {
            for (INode leafNode : expected) {
                assertTrue(actual.hasPair(leafNode.getKey(), leafNode.getValue()));
            }
        }
    }

    public static void assertIterablesMatch(@Nullable final ByteIterable expected, @Nullable final ByteIterable actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertNotNull(actual);

            final ByteIterator expItr = expected.iterator();
            final ByteIterator actItr = actual.iterator();

            while (expItr.hasNext()) {
                assertTrue(actItr.hasNext());

                assertEquals(expItr.next(), actItr.next());
            }

            assertFalse(actItr.hasNext());
        }
    }

    protected static void checkAddressSet(@NotNull final ITree tree, final int count) {
        final LongIterator addressIterator = tree.addressIterator();
        final List<Long> list = new ArrayList<>(count);
        while (addressIterator.hasNext()) {
            long l = addressIterator.next();
            list.add(l);
        }
        Assert.assertEquals(count, list.size());
        Collections.sort(list);
        long prev = -239L;
        for (final long l : list) {
            assertNotEquals(prev, l);
            prev = l;
        }
    }

    protected List<INode> createLNs(int s) {
        return createLNs("v", s);
    }

    protected List<INode> createLNs(String valuePrefix, int s) {
        List<INode> l = new ArrayList<>();
        for (int i = 0; i < s; i++) {
            l.add(kv(i, valuePrefix + i));
        }
        return l;
    }

    protected List<INode> createLNs(String valuePrefix, int s, int u) {
        List<INode> l = new ArrayList<>();
        for (int i = 0; i < s; i++) {
            for (int j = 0; j < u; j++) {
                l.add(kv(i, valuePrefix + i + '#' + j));
            }
        }
        return l;
    }

    protected TreeAwareRunnable checkTree(ITree bt, final int s) {
        return checkTree(bt, "v", s);
    }

    protected TreeAwareRunnable checkTree(ITree bt, final String valuePrefix, final int s) {
        return new TreeAwareRunnable(bt) {
            @Override
            public void run() {
                List<INode> l = createLNs(valuePrefix, s);
                assertMatchesIterator(_t, l);
            }
        };
    }

    protected TreeAwareRunnable checkTree(ITree bt, final int s, final int u) {
        return checkTree(bt, "v", s, u);
    }

    protected TreeAwareRunnable checkTree(ITree bt, final String valuePrefix, final int s, final int u) {
        return new TreeAwareRunnable(bt) {
            @Override
            public void run() {
                List<INode> l = createLNs(valuePrefix, s, u);
                assertMatchesIterator(_t, l);
            }
        };
    }

    public long saveTree() {
        log.beginWrite();
        long result = tm.save();
        log.flush();
        log.endWrite();
        return result;
    }

    public void dump(ITree t) {
        /*t.dump(System.out, new INode.ToString() {
            @Override
            public String toString(INode ln) {
                final StringBuilder sb = new StringBuilder(16);
                sb.append(new String(ln.getKey().getBytesUnsafe(), 0, ln.getKey().getLength()));
                if (ln.hasValue()) {
                    sb.append(':');
                    sb.append(new String(ln.getValue().getBytesUnsafe(), 0, ln.getValue().getLength()));
                }
                return sb.toString();
            }
        });*/
    }

    public static void doDeleteViaCursor(@NotNull TreeBaseTest testCase, @NotNull ByteIterable key) {
        final ITreeCursor cursor = testCase.tm.openCursor();
        assertNotNull(cursor.getSearchKey(key));
        assertTrue(cursor.deleteCurrent());
    }

    public abstract static class TreeAwareRunnable {

        protected ITree _t;

        protected TreeAwareRunnable(ITree t) {
            _t = t;
        }

        protected TreeAwareRunnable() {
        }

        public void setTree(ITree tree) {
            _t = tree;
        }

        public abstract void run();
    }
}
