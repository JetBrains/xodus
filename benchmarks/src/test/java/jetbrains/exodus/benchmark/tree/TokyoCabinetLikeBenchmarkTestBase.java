/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.tree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.NullLoggable;
import org.junit.Before;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class TokyoCabinetLikeBenchmarkTestBase extends TreeBenchmarkTestBase {

    private ByteIterable[] keys;

    @Before
    public void generateKeys() {
        DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        keys = new ByteIterable[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            keys[i] = StringBinding.stringToEntry(FORMAT.format(i));
        }
    }

    protected void fillTree() {
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            tm.add(keys[i], keys[i]);
        }
    }

    protected void shuffleKeys() {
        final List<ByteIterable> _keys = new ArrayList<>(Arrays.asList(keys));
        Collections.shuffle(_keys);
        keys = _keys.toArray(new ByteIterable[TOKYO_CABINET_BENCHMARK_SIZE]);
    }

    @Test
    public void testWrite() {
        long time = TestUtil.time("Write:", new Runnable() {
            @Override
            public void run() {
                fillTree();
                tm.save();
            }
        });

        if (myMessenger != null) {
            myMessenger.putValue("WriteTokyoTest", time);
        }
    }

    @Test
    public void testWriteRandom() {
        shuffleKeys();

        long time = TestUtil.time("Write random: ", new Runnable() {
            @Override
            public void run() {
                fillTree();
                tm.save();
            }
        });

        if (myMessenger != null) {
            myMessenger.putValue("RandomWriteTokyoTest", time);
        }
    }

    @Test
    public void testRead() {
        fillTree();
        final long a = tm.save();
        writePageOfNulls();

        final Runnable read = new Runnable() {
            @Override
            public void run() {
                t = openTree(a, false);
                Cursor c = t.openCursor();

                while (c.getNext()) {
                    iterate(c.getKey());
                    iterate(c.getValue());
                }
            }
        };

        long time = TestUtil.time("Read: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("ReadTokyoTest", time);
        }
        time = TestUtil.time("Read2: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("ReadTokyoTest2", time);
        }
    }

    @Test
    public void testReadRandom() {
        fillTree();
        final long a = tm.save();
        writePageOfNulls();

        shuffleKeys();

        final Runnable read = new Runnable() {
            @Override
            public void run() {
                t = openTree(a, false);
                Cursor c = t.openCursor();

                for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                    c.getSearchKey(keys[i]);
                    iterate(c.getKey());
                    iterate(c.getValue());
                }
            }
        };

        long time = TestUtil.time("Random read: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("RandomReadTokyoTest", time);
        }
        time = TestUtil.time("Random read2: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("RandomReadTokyoTest2", time);
        }
    }

    @Test
    public void testReadRangeRandom() {
        fillTree();
        final long a = tm.save();
        writePageOfNulls();

        shuffleKeys();

        final Runnable read = new Runnable() {
            @Override
            public void run() {
                t = openTree(a, false);
                Cursor c = t.openCursor();

                for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                    c.getSearchKeyRange(keys[i]);
                    iterate(c.getKey());
                    iterate(c.getValue());
                }
            }
        };

        long time = TestUtil.time("Random read: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("RandomReadTokyoTest", time);
        }
        time = TestUtil.time("Random read2: ", read);
        if (myMessenger != null) {
            myMessenger.putValue("RandomReadTokyoTest2", time);
        }
    }

    private void iterate(final ByteIterable iter) {
        final ByteIterator i = iter.iterator();

        while (i.hasNext()) i.next();
    }

    private void writePageOfNulls() {
        for (int i = 0; i < CACHE_PAGE_SIZE; ++i) {
            log.write(NullLoggable.create());
        }
    }

    private static int CACHE_PAGE_SIZE = EnvironmentConfig.DEFAULT.getLogCachePageSize();

    @Override
    protected LogConfig createLogConfig() {
        final LogConfig result = super.createLogConfig();
        result.setSharedCache(true);
        result.setCachePageSize(CACHE_PAGE_SIZE);
        return result;
    }
}

