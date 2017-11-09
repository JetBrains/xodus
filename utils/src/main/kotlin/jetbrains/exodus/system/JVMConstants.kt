/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
@file:Suppress("MemberVisibilityCanPrivate", "HasPlatformType")

package jetbrains.exodus.system

import java.util.*

object JVMConstants {

    val JAVA_SPEC_VERSION = System.getProperty("java.specification.version")
    val JAVA_MAJOR_VERSION: Int
    val JAVA_MINOR_VERSION: Int
    val IS_JAVA8_OR_HIGHER: Boolean
    val IS_JAVA9_OR_HIGHER: Boolean
    val IS_ANDROID = System.getProperty("java.vendor").contains("Android")

    init {
        val st = StringTokenizer(JAVA_SPEC_VERSION, ".")
        JAVA_MAJOR_VERSION = Integer.parseInt(st.nextToken())
        JAVA_MINOR_VERSION = if (st.hasMoreTokens()) Integer.parseInt(st.nextToken()) else 0
        IS_JAVA8_OR_HIGHER = JAVA_MAJOR_VERSION == 1 && JAVA_MINOR_VERSION == 8
        IS_JAVA9_OR_HIGHER = JAVA_MAJOR_VERSION > 1 || (JAVA_MAJOR_VERSION == 1 && JAVA_MINOR_VERSION == 9)
    }
}