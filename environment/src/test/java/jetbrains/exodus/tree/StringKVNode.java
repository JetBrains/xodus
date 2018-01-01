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
package jetbrains.exodus.tree;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 */
public class StringKVNode implements INode {

    @NotNull
    protected final ByteIterable key;
    @NotNull
    protected final ByteIterable value;
    @NotNull
    private final String skey;
    @Nullable
    private final String svalue;

    public StringKVNode(String key, String value) {
        this.key = new ArrayByteIterable(key.getBytes());
        this.value = value == null ? null : new ArrayByteIterable(value.getBytes());
        skey = key;
        svalue = value;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof INode && key.equals(((INode) obj).getKey()));
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @NotNull
    @Override
    public ByteIterable getKey() {
        return key;
    }

    @NotNull
    @Override
    public ByteIterable getValue() {
        return value;
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        for (int i = 1; i < level; i++) out.print(" ");
        out.print("*");
        out.println(renderer == null ? toString() : renderer.toString(this));
    }

    @Override
    public String toString() {
        return "LN {key:" + key + " (" + skey + "), value:" + value + " (" + svalue + ")}";
    }
}
