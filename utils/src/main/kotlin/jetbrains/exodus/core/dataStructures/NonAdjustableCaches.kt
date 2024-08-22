/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures

import jetbrains.exodus.core.execution.SharedTimer


class NonAdjustableConcurrentObjectCache<K, V> @JvmOverloads constructor(size: Int = DEFAULT_SIZE,
                                                                         numberOfGenerations: Int = DEFAULT_NUMBER_OF_GENERATIONS)
    : ConcurrentObjectCache<K, V>(size, numberOfGenerations) {

    override fun getCacheAdjuster(): SharedTimer.ExpirablePeriodicTask? = null
}

class NonAdjustableConcurrentLongObjectCache<V> @JvmOverloads constructor(size: Int = DEFAULT_SIZE,
                                                                          numberOfGenerations: Int = DEFAULT_NUMBER_OF_GENERATIONS)
    : ConcurrentLongObjectCache<V>(size, numberOfGenerations) {

    override fun getCacheAdjuster(): SharedTimer.ExpirablePeriodicTask? = null
}

class NonAdjustableConcurrentIntObjectCache<V> @JvmOverloads constructor(size: Int = DEFAULT_SIZE,
                                                                         numberOfGenerations: Int = DEFAULT_NUMBER_OF_GENERATIONS)
    : ConcurrentIntObjectCache<V>(size, numberOfGenerations) {

    override fun getCacheAdjuster(): SharedTimer.ExpirablePeriodicTask? = null
}