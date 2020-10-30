/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.custom;

import org.jetbrains.annotations.NotNull;

public final class ComparablePair<F extends Comparable<F>, S extends Comparable<S>> implements Comparable<ComparablePair<F, S>> {

    final F first;
    final S second;

    public ComparablePair(@NotNull final F first, @NotNull final S second) {
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
