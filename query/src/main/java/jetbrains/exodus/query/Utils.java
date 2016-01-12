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
package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.metadata.EntityMetaData;
import jetbrains.exodus.entitystore.metadata.ModelMetaData;

public class Utils {

    private Utils() {
    }

    public static <T> int size(final Iterable<T> iterable) {
        int result = 0;
        //noinspection UnusedDeclaration
        for (final T item : iterable) {
            result++;
        }
        return result;
    }

    public static <T> boolean isEmpty(final Iterable<T> iterable) {
        return iterable.iterator().hasNext();
    }

    public static boolean isTypeOf(String type, final String ofType, final ModelMetaData mmd) {
        if (type == null) {
            return false;
        }
        do {
            if (type.equals(ofType)) {
                return true;
            }
            final EntityMetaData emd = mmd.getEntityMetaData(type);
            if (emd == null) {
                break;
            }
            for (final String interface_ : emd.getInterfaceTypes()) {
                if (interface_.equals(ofType)) {
                    return true;
                }
            }
            type = emd.getSuperType();
        } while (type != null);
        return false;
    }
}
