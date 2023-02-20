/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import org.jetbrains.annotations.NotNull;

class EmptyId implements EntityId {
    @Override
    public int getTypeId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLocalId() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    @Override
    public int compareTo(@NotNull EntityId o) {
        if (o instanceof EmptyId) {
            return 0;
        }

        return -1;
    }

    @Override
    public int hashCode() {
        return 72404894;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EmptyId;
    }

    @Override
    public @NotNull String toString() {
        return "EmptyId";
    }
}
