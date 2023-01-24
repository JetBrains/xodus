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
package jetbrains.exodus.query

import jetbrains.exodus.query.metadata.ModelMetaData
import java.lang.Boolean.parseBoolean
import java.lang.Integer.getInteger

internal object Utils {

    @JvmStatic
    val unionSubtypes = parseBoolean(System.getProperty("jetbrains.exodus.query.unionSubtypesResults", "true"))

    @JvmStatic
    val reduceUnionsOfLinksDepth: Int = getInteger("jetbrains.exodus.query.reduceUnionsOfLinksDepth", 4)

    @JvmStatic
    fun safe_equals(left: Any?, right: Any?) = if (left != null) left == right else right == null

    @JvmStatic
    fun isTypeOf(type: String?, ofType: String, mmd: ModelMetaData): Boolean {
        var t: String = type ?: return false
        while (true) {
            if (t == ofType) {
                return true
            }
            val emd = mmd.getEntityMetaData(t) ?: break
            for (i in emd.interfaceTypes) {
                if (i == ofType) {
                    return true
                }
            }
            t = emd.superType ?: break
        }
        return false
    }
}
