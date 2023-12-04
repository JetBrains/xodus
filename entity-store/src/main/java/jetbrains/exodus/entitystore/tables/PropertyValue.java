/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.ComparableValueType;
import org.jetbrains.annotations.NotNull;

public class PropertyValue {

    @NotNull
    private final ComparableValueType type;
    @NotNull
    private final Comparable data;

    public PropertyValue(@NotNull final ComparableValueType type, @NotNull final Comparable data) {
        this.type = type;
        this.data = data;
    }

    @NotNull
    public ComparableValueType getType() {
        return type;
    }

    @NotNull
    public Comparable getData() {
        return data;
    }

    public ComparableBinding getBinding() {
        return type.getBinding();
    }

    public ArrayByteIterable dataToEntry() {
        return getBinding().objectToEntry(data);
    }
}
