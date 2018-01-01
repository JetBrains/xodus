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
package jetbrains.exodus.javascript

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.Environments
import jetbrains.exodus.kotlin.notNull
import org.mozilla.javascript.Context
import org.mozilla.javascript.NativeJavaObject
import org.mozilla.javascript.Scriptable
import java.io.Closeable
import java.io.PrintStream

class Interop(private val rhinoCommand: RhinoCommand,
              private val output: PrintStream,
              private var environment: EnvironmentImpl? = null,
              private var entityStore: PersistentEntityStoreImpl? = null) : Closeable {

    internal var cx: Context? = null
    internal var scope: Scriptable? = null

    fun load(fileName: String) {
        rhinoCommand.evalFileSystemScript(cx.notNull, scope.notNull, fileName)
    }

    fun openEnvironment(location: String) {
        environment?.run { environment = null; close() }
        if (location.isUndefinedOrNull) {
            println("Specify Environment location")
        } else {
            environment = Environments.newInstance(location.replaceBackslashes, envConfig) as EnvironmentImpl
        }
    }

    val env: EnvironmentImpl? get() = environment

    fun openEntityStore(location: String, storeName: String) {
        entityStore?.run { entityStore = null; close() }
        if (location.isUndefinedOrNull) {
            println("Specify EntityStore location")
        } else {
            entityStore = PersistentEntityStores.newInstance(
                    Environments.newInstance(location.replaceBackslashes, envConfig),
                    if (storeName.isUndefinedOrNull) DEFAULT_ENTITY_STORE_NAME else storeName)
        }
    }

    val store: PersistentEntityStoreImpl? get() = entityStore

    val promptString: String get() {
        val env = environment
        if (env != null) {
            return env.location + '>'
        }
        val store = entityStore
        if (store != null) {
            return store.location + '>'
        }
        return ">"
    }

    fun gc(on: Any?): Interop {
        val env = environment ?: return println("Environment is not open.")
        if (on.isUndefinedOrNull) {
            env.gc()
            return println("GC triggered for " + env.location)
        }
        val ec = env.environmentConfig
        if (on is Boolean) {
            ec.isGcEnabled = on
            return println("GC is ${if (on) "on" else "off"}")
        }
        throw IllegalArgumentException(on.toString())
    }

    fun print(o: Any?): Interop {
        val s = when (o) {
            null -> "null"
            is Entity -> entityToString(o)
            is Iterable<*> -> iterableToString(o)
            is NativeJavaObject -> return print(o.unwrap())
            else -> o.toString()
        }
        output.print(s.replace("\n", "\n\r"))
        return flushOutput()
    }

    fun println(o: Any?): Interop = print(o).run { newLine() }

    fun getEntity(id: String): Entity {
        val store = store.notNull
        return store.getEntity(PersistentEntityId.toEntityId(id, store))
    }

    internal fun newLine(): Interop = output.print("\n\r").run { flushOutput() }

    internal fun printPrompt() {
        print(promptString)
    }

    internal fun printChar(c: Char): Interop {
        return if (c == '\n') {
            newLine()
        } else {
            output.print(c)
            flushOutput()
        }
    }

    internal fun backspace(times: Int = 1): Interop {
        repeat(times, {
            output.print(127.toChar())
        })
        return flushOutput()
    }

    internal fun flushOutput(): Interop = apply { output.flush() }

    override fun close() {
        environment?.run { environment = null; close() }
        entityStore?.run { entityStore = null; close() }
    }

    override fun toString() = "RhinoServer interop"

    private companion object {

        private const val DEFAULT_ENTITY_STORE_NAME = "teamsysstore"

        private fun entityToString(entity: Entity) = buildString { entityToStringBuilder(this, entity) }

        private fun entityToStringBuilder(builder: StringBuilder, entity: Entity): StringBuilder {
            builder.append('\n')
            builder.append(entity.type, ' ', entity.id)
            entity.propertyNames.forEach {
                builder.append('\n', it, " = ", entity.getProperty(it))
            }
            entity.linkNames.forEach {
                builder.append('\n', it, " = ", entity.getLink(it))
            }
            return builder
        }

        private fun iterableToString(it: Iterable<*>) = buildString {
            it.forEach {
                if (it is Entity) {
                    entityToStringBuilder(this, it)
                } else {
                    append(it)
                }
                append('\n')
            }
        }

        private val String.replaceBackslashes: String get() = replace('\\', '/')

        private val Any?.isUndefinedOrNull: Boolean get() = this == null || this == "undefined"

        private val envConfig = EnvironmentConfig().setEnvCloseForcedly(true)
    }
}
