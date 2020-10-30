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

import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

public final class ComparablePairBinding extends ComparableBinding {

    private final PropertyTypes propertyTypes;
    private final ComparablePair sample;

    public ComparablePairBinding(@NotNull final PropertyTypes propertyTypes,
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
