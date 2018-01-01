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

import jetbrains.exodus.env.TransactionalExecutable
import org.mozilla.javascript.Context
import org.mozilla.javascript.Script
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.ScriptableObject

internal class EnvironmentRhinoCommand(config: Map<String, *>) : RhinoCommand(config) {

    override fun evalTransactionalScript(cx: Context, script: Script, interop: Interop, scope: Scriptable) {
        val env = interop.env
        if (env != null) {
            env.executeInTransaction(TransactionalExecutable { txn ->
                ScriptableObject.putProperty(scope, TXN_PARAM, Context.javaToJS(txn, scope))
                processScript(interop, script, cx, scope)
            })
        } else {
            processScript(interop, script, cx, scope)
        }
    }

    override fun evalInitScripts(cx: Context, scope: Scriptable) {
        super.evalInitScripts(cx, scope)
        evalResourceScripts(cx, scope, "environments.js")
    }
}