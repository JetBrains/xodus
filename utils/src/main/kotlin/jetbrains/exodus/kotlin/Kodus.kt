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
package jetbrains.exodus.kotlin

val <T> T?.notNull: T get() = this ?: throw IllegalStateException()

fun <T> T?.notNull(msg: () -> Any?) = this ?: throw IllegalStateException(msg().toString())

inline fun <T : Any, R> T.synchronized(block: T.() -> R): R = synchronized(this) {
    return block()
}

