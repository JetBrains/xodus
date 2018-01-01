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
package jetbrains.exodus.core.dataStructures.persistent;

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

public class PersistentStack<T> {

    @SuppressWarnings({"RawUseOfParameterizedType"})
    public static final PersistentStack EMPTY_STACK = new PersistentStack();

    private final T element;
    private final int size;
    private final PersistentStack<T> next;

    private PersistentStack() {
        element = null;
        size = 0;
        next = null;
    }

    private PersistentStack(T e, PersistentStack<T> stack) {
        element = e;
        size = stack.size + 1;
        next = stack;
    }

    public boolean isEmpty() {
        return element == null;
    }

    public int size() {
        return size;
    }

    public PersistentStack<T> push(@NotNull T e) {
        return new PersistentStack<>(e, this);
    }

    public T peek() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return element;
    }

    public PersistentStack<T> skip() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    public PersistentStack<T> reverse() {
        PersistentStack<T> result = new PersistentStack<>();
        for (PersistentStack<T> stack = this; !stack.isEmpty(); stack = stack.skip()) {
            //noinspection ObjectAllocationInLoop
            result = new PersistentStack<>(stack.peek(), result);
        }
        return result;
    }

    @Override
    public int hashCode() {
        return isEmpty() ? 271828182 : element.hashCode() + next.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PersistentStack)) {
            return false;
        }
        PersistentStack<T> stack = (PersistentStack<T>) obj;
        if (isEmpty()) {
            return stack.isEmpty();
        }
        if (stack.isEmpty()) {
            return false;
        }
        return element.equals(stack.element) && next.equals(stack.next);
    }
}
