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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.log.RandomAccessLoggable;
import org.junit.Test;

import java.util.Iterator;

public class PatriciaReclaimTest extends PatriciaTestBase {

    @Test
    public void testSimple() {
        tm = createMutableTree(false, 1);
        tm.put(kv("aab", "aab"));
        tm.put(kv("aabc", "aabc"));
        tm.put(kv("aac", "aac"));
        tm.put(kv("aacd", "aacd"));
        final long a = tm.save();
        t = openTree(a, false);
        tm = t.getMutableCopy();
        final Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(0);
        tm.reclaim(loggables.next(), loggables);
        assertMatches(tm, RM("aa", NM('b', "", "aab", NM('c', "", "aabc")), NM('c', "", "aac", NM('d', "", "aacd"))));
    }

    @Test
    public void testSplitAndReplace() {
        tm = createMutableTree(false, 1);
        tm.put(kv("aaa", "0"));
        tm.put(kv("abbaa", "1"));
        tm.put(kv("aca", "3")); // should be reclaimed
        long a = tm.save();
        t = openTree(a, false);
        tm = t.getMutableCopy();
        tm.delete(key("abbaa"));
        tm.put(kv("abbab", "2"));
        tm.put(kv("abbba", "5"));
        assertMatches(tm, RM("a", N('a', "a", "0"), NM('b', "b", NM('a', "b", "2"), NM('b', "a", "5")), N('c', "a", "3")));
        a = tm.save();
        t = openTree(a, false);
        tm = t.getMutableCopy();
        final Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(0);
        tm.reclaim(loggables.next(), loggables);
        assertMatches(tm, RM("a", NM('a', "a", "0"), N('b', "b", N('a', "b", "2"), N('b', "a", "5")), NM('c', "a", "3")));
    }

    @Test
    public void testSplitBottom() {
        tm = createMutableTree(false, 1);
        tm.put(kv("aaab", "aaab"));
        tm.put(kv("aaac", "aaac"));
        long a = tm.save();
        t = openTree(a, false);
        tm = t.getMutableCopy();
        assertMatches(tm, RM("aaa", N('b', "", "aaab"), N('c', "", "aaac")));
        tm.put(kv("aabb", "aabb"));
        long secondAddress = a + log.read(a).length();
        a = tm.save();
        t = openTree(a, false);
        tm = t.getMutableCopy();
        Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(0);
        tm.reclaim(loggables.next(), loggables);
        assertMatches(tm, RM("aa", NM('a', NM('b', "", "aaab"), NM('c', "", "aaac")), N('b', "b", "aabb")));
        tm = t.getMutableCopy();
        loggables = log.getLoggableIterator(secondAddress);
        tm.reclaim(loggables.next(), loggables);
        assertMatches(tm, RM("aa", NM('a', N('b', "", "aaab"), N('c', "", "aaac")), NM('b', "b", "aabb")));
    }
}
