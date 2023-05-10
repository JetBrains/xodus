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

import com.google.devtools.ksp.isAbstract
import com.google.devtools.ksp.isLocal
import com.google.devtools.ksp.isOpen
import com.google.devtools.ksp.isPrivate
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.*
import com.google.devtools.ksp.visitor.KSTopDownVisitor
import java.nio.file.Path

class JvmFieldEnforcerProcessor(
    private val logger: KSPLogger,
    private val includes: Set<String>,
    private val excludes: Set<String>,
    private val excludePaths: Set<String>
) :
    SymbolProcessor {

    override fun process(resolver: Resolver): List<KSAnnotated> {
        val propertyVisitor = PropertyVisitor()
        resolver.getAllFiles().filter { file ->
            val filePath = Path.of(file.filePath).normalize()
            if (excludePaths.find { path -> filePath.startsWith(Path.of(path).normalize()) } != null) {
                return@filter false
            }

            val packageName = file.packageName.asString()
            (includes.isEmpty() || includes.find { pkg -> packageName.startsWith(pkg) } != null)
                    && excludes.find { pkg -> pkg.startsWith(packageName) } == null
        }.forEach { file ->
            file.accept(propertyVisitor, Unit)
        }

        return emptyList()
    }

    inner class PropertyVisitor : KSTopDownVisitor<Unit, Unit>() {
        override fun visitPropertyDeclaration(property: KSPropertyDeclaration, data: Unit) {
            if (property.isLocal() || property.modifiers.contains(Modifier.CONST)) {
                return
            }

            if (property.isOpen()) {
                logger.error(
                    "Property ${property.qualifiedName?.asString() ?: ""} is open. This is not allowed.", property
                )
            }
            if (property.isDelegated()) {
                logger.error(
                    "Property ${property.qualifiedName?.asString() ?: ""} is delegated. " +
                            "This is not allowed.", property
                )
            }
            if (property.isAbstract()) {
                logger.error(
                    "Property ${property.qualifiedName?.asString() ?: ""} is abstract. " +
                            "This is not allowed.", property
                )
            }
            if (property.modifiers.contains(Modifier.OVERRIDE)) {
                logger.error(
                    "Property ${property.qualifiedName?.asString() ?: ""} overrides another property." +
                            " This is not allowed.", property
                )
            }
            if (property.modifiers.contains(Modifier.LATEINIT)) {
                logger.error(
                    "Property ${property.qualifiedName?.asString() ?: ""} is a lateinit property. This is not allowed.",
                    property
                )
            }

            if (!property.isPrivate()) {
                val annotations = property.annotations
                val annotation =
                    annotations.firstOrNull {
                        it.shortName.asString() == "JvmField"
                    }

                if (annotation == null) {
                    logger.error(
                        "Property ${property.qualifiedName?.asString() ?: ""} is not private and does not have a " +
                                "JvmField annotation. This is not allowed.", property
                    )
                }
            }
        }

        override fun defaultHandler(node: KSNode, data: Unit) {
            //do nothing
        }
    }
}
