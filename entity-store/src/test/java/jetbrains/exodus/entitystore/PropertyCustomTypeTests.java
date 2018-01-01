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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.ByteArrayInputStream;

@SuppressWarnings({"unchecked"})
public class PropertyCustomTypeTests extends EntityStoreTestBase {

    public void testSetGet() {
        final PersistentEntityStoreImpl store = getEntityStore();
        final PropertyTypes propertyTypes = store.getPropertyTypes();
        final PersistentStoreTransaction txn = getStoreTransaction();

        final ComparablePair<Integer, String> sample = new ComparablePair<>(0, "");
        final ComparablePairBinding customBinding = new ComparablePairBinding(propertyTypes, sample);

        // REGISTER CUSTOM TYPE HERE
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);
        Assert.assertTrue(txn.flush());

        final PersistentEntity e = txn.newEntity("CustomType");
        final int count = 1000;
        for (int i = 0; i < count; ++i) {
            e.setProperty(Integer.toString(i), new ComparablePair(i, Integer.toString(i)));
        }
        Assert.assertTrue(txn.flush());
        for (int i = 0; i < count; ++i) {
            final Comparable property = e.getProperty(Integer.toString(i));
            Assert.assertNotNull(property);
            Assert.assertEquals(property, new ComparablePair(i, Integer.toString(i)));
        }
    }

    public void testFind() throws InterruptedException {
        final PersistentEntityStoreImpl store = getEntityStore();
        final PropertyTypes propertyTypes = store.getPropertyTypes();
        final PersistentStoreTransaction txn = getStoreTransaction();

        final ComparablePair<Integer, String> sample = new ComparablePair<>(0, "");
        final ComparablePairBinding customBinding = new ComparablePairBinding(propertyTypes, sample);

        // REGISTER CUSTOM TYPE HERE
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);
        Assert.assertTrue(txn.flush());

        final PersistentEntity e = txn.newEntity("CustomType");
        final int count = 100;
        for (int i = 0; i < count; ++i) {
            e.setProperty(Integer.toString(i), new ComparablePair(i, Integer.toString(i)));
        }
        Assert.assertTrue(txn.flush());
        for (int i = 0; i < count; ++i) {
            final EntityIterable it = txn.find("CustomType", Integer.toString(i), new ComparablePair(i, Integer.toString(i)));
            Assert.assertFalse(it.isEmpty());
            Assert.assertEquals(e, it.getFirst());
            Assert.assertTrue(txn.find("CustomType", Integer.toString(i), new ComparablePair(i, Integer.toString(i + 1))).isEmpty());
            Assert.assertTrue(txn.find("CustomType", Integer.toString(i), new ComparablePair(i + 1, Integer.toString(i))).isEmpty());
            Assert.assertTrue(txn.find("CustomType", Integer.toString(i + 1), new ComparablePair(i, Integer.toString(i))).isEmpty());
            Thread.sleep(1);
        }
    }

    public void testRegisterTwice() throws Exception {
        PersistentEntityStoreImpl store = getEntityStore();
        PropertyTypes propertyTypes = store.getPropertyTypes();
        PersistentStoreTransaction txn = getStoreTransaction();
        ComparablePair<Integer, String> sample = new ComparablePair<>(0, "");
        ComparablePairBinding customBinding = new ComparablePairBinding(propertyTypes, sample);
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);

        reinit();

        store = getEntityStore();
        propertyTypes = store.getPropertyTypes();
        txn = getStoreTransaction();
        sample = new ComparablePair<>(0, "");
        customBinding = new ComparablePairBinding(propertyTypes, sample);
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);
    }

    @TestFor(issues = "XD-603")
    public void testRegisterTwiceWithoutReinit() throws Exception {
        PersistentEntityStoreImpl store = getEntityStore();
        PropertyTypes propertyTypes = store.getPropertyTypes();
        PersistentStoreTransaction txn = getStoreTransaction();
        ComparablePair<Integer, String> sample = new ComparablePair<>(0, "");
        ComparablePairBinding customBinding = new ComparablePairBinding(propertyTypes, sample);
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);
        Assert.assertTrue(txn.flush());
        store.registerCustomPropertyType(txn, sample.getClass(), customBinding);
    }

    private static final class ComparablePair<F extends Comparable<F>, S extends Comparable<S>> implements Comparable<ComparablePair<F, S>> {

        private final F first;
        private final S second;

        private ComparablePair(@NotNull final F first, @NotNull final S second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(@NotNull final ComparablePair<F, S> o) {
            final int result = first.compareTo(o.first);
            return result != 0 ? result : second.compareTo(o.second);
        }

        @Override
        public int hashCode() {
            return first.hashCode() ^ second.hashCode();
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            final ComparablePair<F, S> pair = (ComparablePair<F, S>) obj;
            return first.equals(pair.first) && second.equals(pair.second);
        }

        @Override
        public String toString() {
            return "ComparablePair{" + "first=" + first + ", second=" + second + '}';
        }
    }

    private static final class ComparablePairBinding extends ComparableBinding {

        private final PropertyTypes propertyTypes;
        private final ComparablePair sample;

        private ComparablePairBinding(@NotNull final PropertyTypes propertyTypes,
                                      @NotNull final ComparablePair sample) {
            this.propertyTypes = propertyTypes;
            this.sample = sample;
        }

        @Override
        public Comparable readObject(@NotNull final ByteArrayInputStream stream) {
            return new ComparablePair(
                    propertyTypes.getPropertyType(sample.first.getClass()).getBinding().readObject(stream),
                    propertyTypes.getPropertyType(sample.second.getClass()).getBinding().readObject(stream));
        }

        @Override
        public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
            final ComparablePair cPair = (ComparablePair) object;
            propertyTypes.getPropertyType(cPair.first.getClass()).getBinding().writeObject(output, cPair.first);
            propertyTypes.getPropertyType(cPair.second.getClass()).getBinding().writeObject(output, cPair.second);
        }
    }
}
