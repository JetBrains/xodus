/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.log

import jetbrains.exodus.ByteIterable

/**
 * For writing a loggable to log, instance should provide only type, data and its length.
 * If the instance is read from log, then all its methods return actual data.
 */
interface Loggable {
    /**
     * Address of the loggable in log. It's available (has non-negative value) only for loggables
     * been read from log. Loggables to write return indefinite value.
     *
     * @return non-negative address of the loggable.
     */
    fun getAddress(): Long

    /**
     * Type of loggable.
     *
     * @return integer int the range [0..127] identifying the loggable.
     */
    fun getType(): Byte

    /**
     * @return Length of loggable with header and data
     */
    fun length(): Int

    /**
     * @return address next to end address of loggable
     */
    fun end(): Long

    /**
     * Loggable data.
     *
     * @return loggable data.
     */
    fun getData(): ByteIterable

    /**
     * Length of the loggable data.
     *
     * @return length of the loggable data.
     */
    fun getDataLength(): Int

    /**
     * Returns unique id of structure that the loggable belongs to. Basically, structure id is id of a tree
     * (BTree or Patricia). Valid structure id is non-zero.
     *
     * @return unique structure id.
     */
    fun getStructureId(): Int

    /**
     * Indicates if all loggable completely stored inside single page.
     * This flag is used for optimization of calculation of relative addresses of part of loggable inside
     * implementations of data structures.
     *
     * @return `true` if all loggable data are stored inside single page.
     */
    fun isDataInsideSinglePage(): Boolean

    companion object {
        const val NULL_ADDRESS: Long = -1
        const val NO_STRUCTURE_ID = 0
    }
}
