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

import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

public interface Dumpable {
    /**
     * Dump node to stream
     *
     * @param out      stream to write to
     * @param level    indentation
     * @param renderer renderer for nodes
     */
    void dump(PrintStream out, int level, @Nullable ToString renderer);

    /**
     * To String renderer for INode implementers
     */
    interface ToString {

        String toString(INode ln);
    }
}
