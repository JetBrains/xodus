package jetbrains.exodus.javascript

import jetbrains.exodus.core.dataStructures.NanoSet
import jetbrains.exodus.core.dataStructures.Priority
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransactionalExecutable
import org.apache.sshd.server.Command
import org.apache.sshd.server.Environment
import org.apache.sshd.server.ExitCallback
import org.mozilla.javascript.*
import org.mozilla.javascript.tools.ToolErrorReporter
import java.awt.event.KeyEvent
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.PrintStream

internal class RhinoCommand(private val config: Map<String, *>) : Command, Job() {

    companion object {

        private val INITIAL_SCRIPTS = arrayOf("init.js", "functions.js", "opt.js")
        private val FORBIDDEN_CLASSES = NanoSet("java.lang.System")
        private val START_SCRIPT_PARAM = "startScript"
        private val FINISH_SCRIPT_PARAM = "finishScript"
        private val TXN_PARAM = "txn"
        private val JS_ENGINE_VERSION_PARAM = "jsEngineVersion"
        private val CONFIG_PARAM = "config"
        private val OUTPUT_PARAM = "output"

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
    private var scope: Scriptable? = null

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
                val err = error ?: throw NullPointerException()
                if (line > 0) {
                    err.print("$line: ")
                }
                err.println(message)
                err.flush()
            }
        }
        try {
            processInput(cx)
        } finally {
            try {
                callback?.onExit(0)
                if (scope != null) {
                    val finishScript = config[FINISH_SCRIPT_PARAM]
                    if (finishScript is String) {
                        evalResourceScript(cx, scope, finishScript)
                    }
                }
            } finally {
                stopped = true
                scope = null
                Context.exit()
            }
        }
    }

    private fun processInput(cx: Context) {
        while (!stopped) {
            val sshdOutput = Output()
            if (scope == null) {
                scope = createScope(cx, sshdOutput)
            }
            output?.flush()
            var line = 0
            val source = StringBuilder()
            // Collect lines of source to compile.
            while (true) {
                line++
                sshdOutput.printChar(if (line == 1) '>' else ' ')
                val newline = readLine(sshdOutput) ?: return
                source.append(newline).append('\n')
                if (cx.stringIsCompilableUnit(source.toString())) break
            }
            // execute script in transaction
            try {
                val script = cx.compileString(source.toString(), "<stdin>", line, null)
                if (script != null) {
                    val start = System.currentTimeMillis()
                    val entityStore = getPersistentStore(scope ?: throw NullPointerException())
                    if (entityStore == null) {
                        processScript(sshdOutput, script, cx, scope)
                    } else {
                        entityStore.executeInTransaction(StoreTransactionalExecutable { txn ->
                            ScriptableObject.putProperty(scope, TXN_PARAM, Context.javaToJS(txn, scope))
                            processScript(sshdOutput, script, cx, scope)
                        })
                    }
                    sshdOutput.println("Complete in ${(System.currentTimeMillis() - start)} ms")
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

    private fun evalResourceScript(cx: Context, scope: Scriptable?, name: String) {
        cx.evaluateReader(scope, InputStreamReader(this.javaClass.getResourceAsStream(name)), name, 1, null)
    }

    private fun createScope(cx: Context, output: Output): Scriptable {
        val scope = cx.initStandardObjects(null, true)
        val config = NativeObject()
        this.config.forEach {
            config.defineProperty(it.key, it.value, NativeObject.READONLY)
        }
        config.defineProperty(JS_ENGINE_VERSION_PARAM, cx.implementationVersion, NativeObject.READONLY)
        ScriptableObject.putProperty(scope, CONFIG_PARAM, config)
        ScriptableObject.putProperty(scope, OUTPUT_PARAM, Context.javaToJS(output, scope))
        val startScript = this.config[START_SCRIPT_PARAM]
        if (startScript is String) {
            evalResourceScript(cx, scope, startScript)
        }
        INITIAL_SCRIPTS.forEach { evalResourceScript(cx, scope, it) }
        return scope
    }

    private fun readLine(output: Output): String? {
        val safeInput = input ?: return null
        val s = StringBuilder()
        while (!stopped) {
            val c = safeInput.read()
            if (c == -1 || c == 3) break
            val ch = c.toChar()
            if (ch == '\n' || ch == '\r') {
                output.newLine()
                break
            }
            // delete
            if (c == 127 && s.isNotEmpty()) {
                s.deleteCharAt(s.length - 1)
                output.printChar('\b')
            }
            if (isPrintableChar(ch)) {
                output.printChar(c.toChar())
                s.append(c.toChar())
            }
        }
        return s.toString()
    }

    private fun processScript(output: Output, script: Script, cx: Context, scope: Scriptable?) {
        val result = script.exec(cx, scope)
        if (result !== Context.getUndefinedValue()) {
            output.println(Context.toString(result))
        }
    }

    private fun getPersistentStore(scope: Scriptable): PersistentEntityStore? {
        val store = scope.get("store", null)
        if (store == null || store === UniqueTag.NOT_FOUND) {
            return null
        }
        return Context.jsToJava(store, PersistentEntityStore::class.java) as PersistentEntityStore
    }

    private fun toPrintStream(out: OutputStream?): PrintStream? {
        return if (out == null) null else PrintStream(out)
    }

    @Suppress("unused")
    inner class Output {

        override fun toString(): String {
            return "SSHD Output"
        }

        fun print(s: String?) {
            output?.print(s?.replace("\n", "\n\r"))
            flushOutput()
        }

        fun println(s: String) {
            print(s + '\n')
        }

        fun newLine() {
            output?.print("\n\r")
            flushOutput()
        }

        fun printChar(s: Char) {
            if (s == '\n') {
                newLine()
            } else {
                output?.print(s)
                flushOutput()
            }
        }

        private fun flushOutput() {
            output?.flush()
        }
    }
}