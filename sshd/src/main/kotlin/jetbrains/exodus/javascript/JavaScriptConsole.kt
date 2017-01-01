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
package jetbrains.exodus.javascript

import jetbrains.exodus.core.dataStructures.hash.HashMap
import org.apache.commons.cli.*
import kotlin.system.exitProcess

// todo: remove this after XD-563 is fixed
fun main(args: Array<String>) {

    fun getCommandLine(args: Array<String>): CommandLine {

        fun createOption(description: String, opt: Char, type: Any? = null): Option {
            OptionBuilder.hasArg()
            if (type != null) {
                OptionBuilder.withType(type)
            }
            OptionBuilder.withDescription(description)
            return OptionBuilder.create(opt)
        }

        val options = Options()
        options.addOption(createOption("sshd port", 'l', Number::class.java))
        options.addOption(createOption("password to login", 'p'))
        options.addOption(createOption("xodus database path", 'x'))
        if (args.isEmpty()) {
            val formatter = HelpFormatter()
            formatter.printHelp("JavaScriptConsole", options)
            exitProcess(0)
        }
        val parser = BasicParser()
        return parser.parse(options, args)
    }

    val line = getCommandLine(args)
    val portValue = line.getParsedOptionValue("l")
    val port: Int = if (portValue == null) 2808 else portValue as Int
    val password = line.getOptionValue('p', null)
    val config = HashMap<String, Any>()
    config.put("location", line.getOptionValue('x', null))
    RhinoServer(config, port, password)
}