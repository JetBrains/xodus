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
package jetbrains.exodus.log;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

/**
 * For writing a loggable to log, instance should provide only type, data and its length.
 * <p/>
 * If the instance is read from log, then all its methods return actual data.
 */
public interface Loggable {

    long NULL_ADDRESS = -1;
    int NO_STRUCTURE_ID = 0;

    /**
     * Address of the loggable in log. It's available (has non-negative value) only for loggables
     * been read from log. Loggables to write return indefinite value.
     *
     * @return non-negative address of the loggable.
     */
    long getAddress();

    /**
     * Type of loggable.
     *
     * @return integer int the range [0..127] identifying the loggable.
     */
    byte getType();

    /**
     * Length of the loggable with header and data.
     *
     * @return length of the loggable with header and data.
     */
    int length();

    /**
     * Loggable data.
     *
     * @return loggable data.
     */
    @NotNull
    ByteIterable getData();

    /**
     * Length of the loggable data.
     *
     * @return length of the loggable data.
     */
    int getDataLength();

    /**
     * Returns unique id of structure that the loggable belongs to. Basically, structure id is id of a tree
     * (BTree or Patricia). Valid structure id is non-zero.
     *
     * @return unique structure id.
     */
    int getStructureId();
}
