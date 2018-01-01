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

import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"HardCodedStringLiteral"})
public class Priority implements Comparable<Priority> {

    public static final Priority highest = new Priority(Integer.MAX_VALUE / 2, "The highest possible priority");
    public static final Priority above_normal = new Priority(Integer.MAX_VALUE / 4, "The above normal priority");
    public static final Priority normal = new Priority(0, "The normal (default) priority");
    public static final Priority below_normal = new Priority(-above_normal.value, "The below normal priority");
    public static final Priority lowest = new Priority(-highest.value, "The lowest possible priority");

    private final int value;
    @Nullable
    private final String description;

    private Priority(final int value, @Nullable final String description) {
        this.value = value;
        this.description = description;
    }

    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Priority)) return false;
        final Priority priority = (Priority) o;
        return value == priority.value;
    }

    public int hashCode() {
        return value;
    }

    @Override
    public int compareTo(final Priority o) {
        return value - o.value;
    }

    @Nullable
    public final String getDescription() {
        return description;
    }

    public static Priority mean(final Priority p1, final Priority p2) {
        // use long in order to avoid integer overflow
        final long value = ((long) p1.value + (long) p2.value) >>> 1;
        return new Priority((int) value, null);
    }
}
