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
package jetbrains.xodus

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider

class JvmFieldEnforcerProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        val includes = if (environment.options["includes"] != null) {
            environment.options["includes"]!!.split(",").toSet()
        } else {
            emptySet()
        }

        val excludes = if (environment.options["excludes"] != null) {
            environment.options["excludes"]!!.split(",").toSet()
        } else {
            emptySet()
        }

        val excludePaths = if (environment.options["excludePaths"] != null) {
            environment.options["excludePaths"]!!.split(",").toSet()
        } else {
            emptySet()
        }

        return JvmFieldEnforcerProcessor(environment.logger, includes, excludes, excludePaths)
    }
}