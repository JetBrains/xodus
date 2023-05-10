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