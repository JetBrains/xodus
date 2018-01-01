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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.execution.locks.Guard;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class PriorityQueue<P extends Comparable<? super P>, E> implements Iterable<E> {

    public abstract boolean isEmpty();

    public abstract int size();

    public abstract E push(@NotNull P priority, @NotNull E value);

    @Nullable
    public final E peek() {
        final Pair<P, E> pair = peekPair();
        return pair == null ? null : pair.getSecond();
    }

    @Nullable
    public abstract Pair<P, E> peekPair();

    @Nullable
    public abstract Pair<P, E> floorPair();

    @Nullable
    public abstract E pop();

    public abstract void clear();

    public abstract Guard lock();

    public abstract void unlock();
}
