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
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.hash.HashSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class PersistentObjectCacheTest {

    @Test
    public void cacheFiniteness() {
        final PersistentObjectCache<String, String> cache = new PersistentObjectCache<>(4);
        cache.put("Eclipse", "An IDE");
        cache.put("IDEA", "good");
        cache.put("IDEA 4.5", "better");
        Assert.assertNotNull(cache.get("IDEA"));
        Assert.assertNotNull(cache.get("IDEA 4.5"));
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache.get("Eclipse"));
        Assert.assertNotNull(cache.get("IDEA 4.5"));
        Assert.assertNotNull(cache.get("IDEA"));
        cache.put("IDEA 5.0", "perfect");
        cache.put("IDEA 6.0", "ideal");
        Assert.assertNotNull(cache.getObject("IDEA 6.0"));
        Assert.assertNotNull(cache.getObject("IDEA 5.0"));
        Assert.assertNotNull(cache.getObject("IDEA 4.5"));
        Assert.assertNotNull(cache.getObject("IDEA"));
    }

    @Test
    public void cacheIterator() {
        final PersistentObjectCache<String, String> cache = new PersistentObjectCache<>(4);
        cache.put("Eclipse", "An IDE");
        cache.put("IDEA", "good IDEA");
        cache.put("IDEA 4.5", "better IDEA");
        Assert.assertNotNull(cache.get("IDEA"));
        Assert.assertNotNull(cache.get("IDEA 4.5"));
        cache.put("IDEA 5.0", "perfect IDEA");
        cache.put("IDEA 6.0", "IDEAL");
        HashSet<String> values = new HashSet<>();
        final Iterator<String> it = cache.values();
        while (it.hasNext()) {
            values.add(it.next());
        }
        Assert.assertNull(cache.get("Eclipse"));
        Assert.assertFalse(values.contains("An IDE"));
        Assert.assertTrue(values.contains("good IDEA"));
        Assert.assertTrue(values.contains("better IDEA"));
        Assert.assertTrue(values.contains("perfect IDEA"));
        Assert.assertTrue(values.contains("IDEAL"));
    }

    @Test
    public void getClone() {
        final PersistentObjectCache<String, String> cache = new PersistentObjectCache<>(4);
        cache.put("IDEA", "good IDEA");
        cache.put("NetBeans", "bad IDEA");
        final PersistentObjectCache<String, String> copy = cache.getClone(null);
        Assert.assertNotNull(copy.get("IDEA"));
        copy.put("Eclipse", "An IDE");
        Assert.assertNull(cache.get("Eclipse"));
        Assert.assertNotNull(cache.get("NetBeans"));
        Assert.assertNotNull(copy.get("IDEA"));
        Assert.assertNotNull(copy.remove("NetBeans"));
        Assert.assertNull(copy.get("NetBeans"));
        Assert.assertNotNull(cache.get("NetBeans"));

    }
}
