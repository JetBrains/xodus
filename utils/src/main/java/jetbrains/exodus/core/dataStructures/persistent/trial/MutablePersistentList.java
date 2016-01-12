/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures.persistent.trial;

import jetbrains.exodus.core.dataStructures.hash.HashSet;

class MutablePersistentList<T> extends PersistentList<T> {
    protected HashSet<Object[]> allocated = new HashSet<>();
    protected int hits;
    protected int misses;

    public MutablePersistentList(PersistentList<T> source) {
        super(source.size, source.shift, mutableRoot(source.root), (T[]) mutableRecent(source.recent));
        allocated.add(root);
        allocated.add(recent);
    }

    @Override
    protected PersistentList<T> maybeEmpty() {
        return this;
    }

    @Override
    protected Object[] ensureMutable(Object[] parent) {
        if (allocated.contains(parent)) {
            hits++;
            return parent;
        }
        misses++;
        final Object[] result = super.ensureMutable(parent);
        allocated.add(result);
        return result;
    }

    @Override
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    protected PersistentList<T> mutableCopy(int newSize, int newShift, Object[] newRoot, T[] newRecent) {
        size = newSize;
        shift = newShift;
        root = newRoot;
        recent = newRecent;
        return this;
    }

    private static Object[] mutableRoot(Object[] root) {
        return root.clone();
    }

    private static Object[] mutableRecent(Object[] recent) {
        final Object[] result = new Object[32];
        System.arraycopy(recent, 0, result, 0, recent.length);
        return result;
    }

    public static void main(String[] args) {
        PersistentList<String> l = PersistentList.EMPTY.toMutable();
        for (int i = 0; i < 9000; i++) {
            l = l.add("test " + i);
        }
        for (int i = 0; i < 9000; i++) {
            l = l.pop();
        }
        System.out.println(l.size);
    }
}
