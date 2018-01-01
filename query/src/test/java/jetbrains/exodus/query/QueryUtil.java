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
package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

@SuppressWarnings({"IfStatementWithTooManyBranches", "rawtypes", "ClassNamingConvention"})
public class QueryUtil {

    private static final float FLOAT_PRECISION = 0.0001f;
    private static final double DOUBLE_PRECISION = 0.0000001f;

    public static int getSize(Iterable<Entity> it) {
        if (it == null) {
            return 0;
        }
        if (it instanceof StaticTypedEntityIterable) {
            it = ((StaticTypedEntityIterable) it).instantiate();
        }
        if (it == EntityIterableBase.EMPTY) {
            return 0;
        }
        if (it instanceof EntityIterable) {
            return (int) ((EntityIterable) it).size();
        }
        if (it instanceof Collection) {
            return ((Collection) it).size();
        }
        int i = 0;
        for (final Entity ignored : it) {
            i++;
        }
        return i;
    }

    public static boolean isEmpty(QueryEngine engine, Iterable<Entity> it) {
        if (it == null) {
            return true;
        }
        if (it instanceof Collection) {
            return ((Collection) it).isEmpty();
        }
        it = engine.toEntityIterable(it);
        if (engine.isPersistentIterable(it)) {
            return ((EntityIterable) it).isEmpty();
        }
        return !it.iterator().hasNext();
    }

    public static Entity getFirst(QueryEngine engine, Iterable<Entity> it) {
        it = engine.toEntityIterable(it);
        if (engine.isPersistentIterable(it)) {
            return ((EntityIterableBase) it).getFirst();
        }
        return it.iterator().next();
    }

    public static Comparable nextGreater(@NotNull final Comparable value, @NotNull final Class clazz) {
        if (Integer.class.equals(clazz)) {
            return ((Integer) value) + 1;
        }
        if (Long.class.equals(clazz)) {
            return ((Long) value) + 1;
        }
        if (Float.class.equals(clazz)) {
            float result;
            float addend = FLOAT_PRECISION;
            do {
                result = (Float) value + addend;
                addend *= 2;
            } while (value.equals(result));
            return result;
        }
        if (Double.class.equals(clazz)) {
            double result;
            double addend = DOUBLE_PRECISION;
            do {
                result = (Double) value + addend;
                addend *= 2;
            } while (value.equals(result));
            return result;
        }
        if (Short.class.equals(clazz)) {
            return ((Short) value) + 1;
        }
        if (Byte.class.equals(clazz)) {
            return ((Byte) value) + 1;
        }
        if (Boolean.class.equals(clazz)) {
            return Boolean.TRUE;
        }
        return null;
    }

    public static Comparable previousLess(@NotNull final Comparable value, @NotNull final Class clazz) {
        if (Integer.class.equals(clazz)) {
            return ((Integer) value) - 1;
        }
        if (Long.class.equals(clazz)) {
            return ((Long) value) - 1;
        }
        if (Float.class.equals(clazz)) {
            float result;
            float subtrahend = FLOAT_PRECISION;
            do {
                result = (Float) value - subtrahend;
                subtrahend *= 2;
            } while (value.equals(result));
            return result;
        }
        if (Double.class.equals(clazz)) {
            double result;
            double subtrahend = DOUBLE_PRECISION;
            do {
                result = (Double) value - subtrahend;
                subtrahend *= 2;
            } while (value.equals(result));
            return result;
        }
        if (Short.class.equals(clazz)) {
            return ((Short) value) - 1;
        }
        if (Byte.class.equals(clazz)) {
            return ((Byte) value) - 1;
        }
        if (Boolean.class.equals(clazz)) {
            return Boolean.FALSE;
        }
        return null;
    }

    public static Comparable positiveInfinity(@NotNull final Class clazz) {
        if (Integer.class.equals(clazz)) {
            return Integer.MAX_VALUE;
        }
        if (Long.class.equals(clazz)) {
            return Long.MAX_VALUE;
        }
        if (Float.class.equals(clazz)) {
            return Float.MAX_VALUE;
        }
        if (Double.class.equals(clazz)) {
            return Double.MAX_VALUE;
        }
        if (Short.class.equals(clazz)) {
            return Short.MAX_VALUE;
        }
        if (Byte.class.equals(clazz)) {
            return Byte.MAX_VALUE;
        }
        if (Boolean.class.equals(clazz)) {
            return Boolean.TRUE;
        }
        return null;
    }

    public static Comparable negativeInfinity(@NotNull final Class clazz) {
        if (Integer.class.equals(clazz)) {
            return Integer.MIN_VALUE;
        }
        if (Long.class.equals(clazz)) {
            return Long.MIN_VALUE;
        }
        if (Float.class.equals(clazz)) {
            return -Float.MAX_VALUE;
        }
        if (Double.class.equals(clazz)) {
            return -Double.MAX_VALUE;
        }
        if (Short.class.equals(clazz)) {
            return Short.MIN_VALUE;
        }
        if (Byte.class.equals(clazz)) {
            return Byte.MIN_VALUE;
        }
        if (Boolean.class.equals(clazz)) {
            return Boolean.FALSE;
        }
        return null;
    }
}
