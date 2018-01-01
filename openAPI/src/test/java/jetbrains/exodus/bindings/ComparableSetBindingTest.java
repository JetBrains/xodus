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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ExodusException;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class ComparableSetBindingTest {

    @Test(expected = ExodusException.class)
    public void empty() {
        ComparableSetBinding.comparableSetToEntry(new ComparableSet());
    }

    @Test(expected = ClassCastException.class)
    public void mixingTypes() {
        final ComparableSet set = new ComparableSet<>();
        set.addItem(0);
        set.addItem("0");
    }

    @Test
    public void strings() {
        final ComparableSet<String> set = new ComparableSet<>();
        set.addItem("Xodus");
        set.addItem("is");
        set.addItem("a");
        set.addItem("transactional");
        set.addItem("schema-less");
        set.addItem("embedded");
        set.addItem("database");
        set.addItem("written");
        set.addItem("in");
        set.addItem("pure");
        set.addItem("Java");
        Assert.assertFalse(set.addItem("Java"));
        final ComparableSet<String> copy = ComparableSetBinding.entryToComparableSet(ComparableSetBinding.comparableSetToEntry(set));
        Assert.assertTrue(set.isDirty());
        Assert.assertFalse(copy.isDirty());
        Assert.assertEquals(set, copy);
        copy.addItem("not found");
        Assert.assertNotEquals(set, copy);
    }

    @Test
    public void numbers() {
        final ComparableSet<Integer> set = new ComparableSet<>();
        set.addItem(0);
        set.addItem(-12345);
        set.addItem(987);
        set.addItem(5);
        set.addItem(6);
        set.addItem(7);
        Assert.assertFalse(set.addItem(0));
        final ComparableSet<Integer> copy = ComparableSetBinding.entryToComparableSet(ComparableSetBinding.comparableSetToEntry(set));
        Assert.assertTrue(set.isDirty());
        Assert.assertFalse(copy.isDirty());
        Assert.assertEquals(set, copy);
        copy.addItem(Integer.MAX_VALUE);
        Assert.assertNotEquals(set, copy);
    }

    @Test
    public void remove() {
        final ComparableSet<Integer> set = new ComparableSet<>();
        set.addItem(0);
        set.addItem(-12345);
        set.addItem(987);
        set.addItem(5);
        set.addItem(6);
        set.addItem(7);
        Assert.assertFalse(set.addItem(0));
        Assert.assertFalse(set.removeItem(Integer.MAX_VALUE));
        final ComparableSet<Integer> copy = ComparableSetBinding.entryToComparableSet(ComparableSetBinding.comparableSetToEntry(set));
        Assert.assertTrue(set.isDirty());
        Assert.assertFalse(copy.isDirty());
        Assert.assertEquals(set, copy);
        Assert.assertTrue(copy.removeItem(0));
        Assert.assertNotEquals(set, copy);
    }

    @Test
    public void testCaseInsensitive() {
        final ComparableSet<String> set1 = new ComparableSet<>();
        set1.addItem("xodus");
        final ComparableSet<String> set2 = new ComparableSet<>();
        set2.addItem("Xodus");
        Assert.assertEquals(0, set1.compareTo(set2));
    }
}
