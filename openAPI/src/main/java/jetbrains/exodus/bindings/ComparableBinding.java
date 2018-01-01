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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.util.ByteIterableUtil;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * Bindings are used to represent comparable {@code Java} objects as {@linkplain ByteIterable}. There are several
 * inheritors of the {@code ComparableBinding} class, they allow to serialize a value of any Java primitive type or
 * {@linkplain String} to {@linkplain ArrayByteIterable}, as well as to deserialize {@linkplain ByteIterable} to a value.
 * All the inheritors contain two static methods: one for getting {@linkplain ByteIterable} entry from a value,
 * and another for getting value from a {@linkplain ByteIterable} entry.
 *
 * <p>Bindings save the order of values. This means that the greater the value, the greater the {@code ByteIterable}
 * entry. The order of the {@code ByteIterable} entries is defined by
 * {@linkplain ByteIterableUtil#compare(ByteIterable, ByteIterable)}.
 *
 * @see BooleanBinding
 * @see ByteBinding
 * @see ComparableSetBinding
 * @see DoubleBinding
 * @see FloatBinding
 * @see IntegerBinding
 * @see LongBinding
 * @see ShortBinding
 * @see StringBinding
 * @see ByteIterable
 * @see ArrayByteIterable
 */
public abstract class ComparableBinding {

    public final Comparable entryToObject(@NotNull final ByteIterable entry) {
        return readObject(new ByteArrayInputStream(entry.getBytesUnsafe(), 0, entry.getLength()));
    }

    public final ArrayByteIterable objectToEntry(@NotNull final Comparable object) {
        final LightOutputStream output = new LightOutputStream();
        writeObject(output, object);
        return output.asArrayByteIterable();
    }

    public abstract Comparable readObject(@NotNull final ByteArrayInputStream stream);

    public abstract void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object);

}
