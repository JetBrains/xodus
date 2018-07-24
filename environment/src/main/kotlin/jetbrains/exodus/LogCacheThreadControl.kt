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
package jetbrains.exodus

object LogCacheThreadControl {

    private val cachingOff: ThreadLocal<Boolean?> = ThreadLocal()

    val isCachingOn: Boolean get() = cachingOff.get().run { return !(this ?: false) }

    fun setCachingOn(on: Boolean) = if (on) cachingOff.remove() else cachingOff.set(true)

    fun withNoCaching(block: () -> Unit) {
        val initialState = isCachingOn
        setCachingOn(false)
        try {
            block()
        } finally {
            setCachingOn(initialState)
        }
    }
}
