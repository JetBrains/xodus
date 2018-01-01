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

import jetbrains.exodus.core.dataStructures.NanoSet
import jetbrains.exodus.core.dataStructures.Priority
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.kotlin.notNull
import org.apache.sshd.server.Command
import org.apache.sshd.server.Environment
import org.apache.sshd.server.ExitCallback
import org.mozilla.javascript.*
import org.mozilla.javascript.tools.ToolErrorReporter
import java.awt.event.KeyEvent
import java.io.*

abstract class RhinoCommand(protected val config: Map<String, *>) : Job(), Command {

    companion object {

        internal fun createCommand(config: Map<String, *>): RhinoCommand {
            return when (config[API_LAYER]) {
                ENVIRONMENTS -> EnvironmentRhinoCommand(config)
                ENTITY_STORES -> EntityStoreRhinoCommand(config)
                else -> throw IllegalArgumentException("The value of apiLayer should be 'Environments' or 'EntityStores'")
            }
        }

        const val TXN_PARAM = "txn"
        const val API_LAYER = "apiLayer"
        const val ENVIRONMENTS = "environments"
        const val ENTITY_STORES = "entitystores"
        const val CONSOLE = "console"
        const val ENVIRONMENT_INSTANCE = "environment instance"
        const val ENTITY_STORE_INSTANCE = "entity store instance"
        private const val JS_ENGINE_VERSION_PARAM = "jsEngineVersion"
        private const val CONFIG_PARAM = "config"
        private const val INTEROP_PARAM = "interop"
        private val FORBIDDEN_CLASSES = NanoSet("java.lang.System")
        private const val RECENT_COMMANDS_LIST_MAX_SIZE = 1000

        private fun isPrintableChar(c: Char): Boolean {
            val block = Character.UnicodeBlock.of(c)
            return !Character.isISOControl(c) &&
                    c != KeyEvent.CHAR_UNDEFINED &&
                    block != null &&
                    block !== Character.UnicodeBlock.SPECIALS
        }
    }

    private var callback: ExitCallback? = null
    private var input: InputStream? = null
    private var stopped = false
    private var output: PrintStream? = null
    private var error: PrintStream? = null
    private val recentCommands: MutableList<String> = arrayListOf()

    override fun setExitCallback(callback: ExitCallback?) {
        this.callback = callback
    }

    override fun setInputStream(input: InputStream?) {
        this.input = input
    }

    override fun destroy() {
        stopped = true
    }

    override fun setErrorStream(err: OutputStream?) {
        error = toPrintStream(err)
    }

    override fun setOutputStream(out: OutputStream?) {
        output = toPrintStream(out)
    }

    override fun start(env: Environment?) {
        processor = RhinoServer.commandProcessor
        queue(Priority.normal)
    }

    override fun execute() {
        val cx = Context.enter()
        cx.setClassShutter { !FORBIDDEN_CLASSES.contains(it) }
        cx.errorReporter = object : ErrorReporter {
            override fun warning(message: String?, sourceName: String?, line: Int, lineSource: String?, lineOffset: Int) {
                // ignore
            }

            override fun runtimeError(message: String?, sourceName: String?, line: Int, lineSource: String?, lineOffset: Int): EvaluatorException {
                return EvaluatorException(message, sourceName, line, lineSource, lineOffset)
            }

            override fun error(message: String?, sourceName: String?, line: Int, lineSource: String?, lineOffset: Int) {
                val out = output.notNull
                if (sourceName != null) {
                    out.print("$sourceName: ")
                }
                if (line > 0) {
                    out.print("$line: ")
                }
                out.println(message)
                out.flush()
            }
        }
        try {
            processInput(cx)
        } finally {
            try {
                callback?.onExit(0)
            } finally {
                stopped = true
                Context.exit()
            }
        }
    }

    protected fun evalResourceScripts(cx: Context, scope: Scriptable, vararg names: String) {
        names.forEach {
            cx.evaluateReader(scope, InputStreamReader(this.javaClass.getResourceAsStream(it)), it, 1, null)
        }
    }

    fun evalFileSystemScript(cx: Context, scope: Scriptable, fileName: String) {
        cx.evaluateReader(scope, FileReader(fileName), fileName, 1, null)
    }

    protected fun processScript(interop: Interop, script: Script, cx: Context, scope: Scriptable?) {
        val result = script.exec(cx, scope)
        if (result !== Context.getUndefinedValue()) {
            interop.println(result)
        }
    }

    protected open fun evalInitScripts(cx: Context, scope: Scriptable) {
        evalResourceScripts(cx, scope, "bindings.js", "functions.js")
    }

    protected abstract fun evalTransactionalScript(cx: Context, script: Script, interop: Interop, scope: Scriptable)

    private fun processInput(cx: Context) {
        Interop(this, output.notNull,
                config[ENVIRONMENT_INSTANCE] as? EnvironmentImpl,
                config[ENTITY_STORE_INSTANCE] as? PersistentEntityStoreImpl).use { interop ->
            interop.flushOutput()
            val scope = createScope(cx, interop)
            evalInitScripts(cx, scope)
            while (!stopped) {
                interop.printPrompt()
                val cmd = buildString { readLine(this, interop) }
                try {
                    if (cmd.isNotBlank()) {
                        // exit
                        if (cmd.equals("exit", true) || cmd.equals("quit", true)) {
                            if (isConsole) {
                                interop.println("Press Enter to $cmd...")
                            }
                            break
                        }
                        recentCommands.add(cmd)
                        if (recentCommands.size > RECENT_COMMANDS_LIST_MAX_SIZE) {
                            recentCommands.removeAt(0)
                        }
                        val script = cx.compileString(ScriptPreprocessor.preprocess { cmd }, "<stdin>", 0, null)
                        if (script != null) {
                            val start = System.currentTimeMillis()
                            // execute script in transaction if an Environment or an EntityStore is open
                            evalTransactionalScript(cx, script, interop, scope)
                            interop.println("Completed in ${(System.currentTimeMillis() - start)} ms")
                        }
                    }
                } catch (rex: RhinoException) {
                    ToolErrorReporter.reportException(cx.errorReporter, rex)

                } catch (ex: VirtualMachineError) {
                    // Treat StackOverflow and OutOfMemory as runtime errors
                    ex.printStackTrace()
                    Context.reportError(ex.toString())
                }
            }
        }
    }

    private fun createScope(cx: Context, interop: Interop): Scriptable {
        interop.cx = cx
        val scope = cx.initStandardObjects(null, true)
        interop.scope = scope
        val config = NativeObject()
        this.config.forEach {
            config.defineProperty(it.key, it.value, NativeObject.READONLY)
        }
        config.defineProperty(JS_ENGINE_VERSION_PARAM, cx.implementationVersion, NativeObject.READONLY)
        ScriptableObject.putProperty(scope, CONFIG_PARAM, config)
        ScriptableObject.putProperty(scope, INTEROP_PARAM, Context.javaToJS(interop, scope))
        return scope
    }

    private fun readLine(s: StringBuilder, interop: Interop) {
        val inp = input ?: return
        var cmdIdx = recentCommands.size
        while (!stopped) {
            val c = inp.read()
            if (c == -1 || c == 3 || c == 26) break
            val ch = c.toChar()
            if (ch == '\n') {
                if (!isConsole) {
                    interop.newLine()
                }
                break
            }
            when (c) {
            // backspace
                127 ->
                    if (s.isNotEmpty()) {
                        s.deleteCharAt(s.length - 1)
                        interop.backspace()
                    }
            // up or down
                27 -> {
                    inp.read()
                    var newCmdIdx = cmdIdx
                    val upOrDown = inp.read()
                    if (upOrDown == 65) {
                        --newCmdIdx
                    } else if (upOrDown == 66) {
                        ++newCmdIdx
                    }
                    if (newCmdIdx >= 0 && newCmdIdx <= recentCommands.size && newCmdIdx != cmdIdx) {
                        cmdIdx = newCmdIdx
                        if (s.isNotEmpty()) {
                            interop.backspace(s.length)
                            s.delete(0, s.length)
                        }
                        if (newCmdIdx < recentCommands.size) {
                            s.append(recentCommands[cmdIdx])
                        }
                        interop.print(s)
                    }
                }
                else ->
                    if (isPrintableChar(ch)) {
                        if (!isConsole) {
                            interop.printChar(ch)
                        }
                        s.append(ch)
                    }
            }
        }
    }

    private fun toPrintStream(out: OutputStream?): PrintStream? {
        return if (out == null) null else PrintStream(out)
    }

    private val isConsole = true == config[CONSOLE]
}