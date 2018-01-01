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

import java.util.ArrayList;

@SuppressWarnings({"CloneableClassInSecureContext", "CloneableClassWithoutClone", "ClassExtendsConcreteCollection"})
public class Stack<T> extends ArrayList<T> {

    private T last;

    public void push(T t) {
        if (last != null) {
            add(last);
        }
        last = t;
    }

    public T peek() {
        return last;
    }

    public T pop() {
        final T result = last;
        if (result != null) {
            last = super.isEmpty() ? null : remove(super.size() - 1);
        }
        return result;
    }

    @Override
    public int size() {
        return last == null ? 0 : super.size() + 1;
    }

    @Override
    public boolean isEmpty() {
        return last == null;
    }
}
