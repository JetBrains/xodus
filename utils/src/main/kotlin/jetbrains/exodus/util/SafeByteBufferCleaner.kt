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
package jetbrains.exodus.util

import jetbrains.exodus.system.JVMConstants
import jetbrains.exodus.util.UnsafeHolder.doPrivileged
import jetbrains.exodus.util.UnsafeHolder.theUnsafe
import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.security.PrivilegedActionException

object SafeByteBufferCleaner {

    private val invokeCleanerMethod = doPrivileged {
        try {
            UnsafeHolder.unsafeClass.getDeclaredMethod("invokeCleaner", ByteBuffer::class.java).apply { isAccessible = true }
        } catch (t: Throwable) {
            null
        }
    }
    private val directByteBufferClass: Class<*> = doPrivileged { Class.forName("java.nio.DirectByteBuffer") }
    private val dbbFreeMethod = doPrivileged { getDirectByteBufferFreeMethod() }
    private val dbbCleanerMethod = doPrivileged { getDirectByteBufferCleanerMethod() }
    private val cleanMethod = doPrivileged {
        if (dbbCleanerMethod == null) null else {
            try {
                dbbCleanerMethod.returnType.getDeclaredMethod("clean").apply { isAccessible = true }
            } catch (t: Throwable) {
                null
            }
        }
    }

    fun clean(buffer: ByteBuffer) {
        try {
            doPrivileged {
                if (JVMConstants.IS_JAVA9_OR_HIGHER && invokeCleanerMethod != null) {
                    // JDK9 or higher
                    try {
                        invokeCleanerMethod.invoke(theUnsafe, buffer)
                        return@doPrivileged
                    } catch (t: Throwable) {
                        throw RuntimeException(t)
                    }
                }
                if (buffer.javaClass.simpleName == "MappedByteBufferAdapter") {
                    if (!JVMConstants.IS_ANDROID) {
                        throw RuntimeException("MappedByteBufferAdapter only supported for Android")
                    }
                    // for Android 4.1 try to call ((MappedByteBufferAdapter)buffer).free()
                    dbbFreeMethod?.invoke(buffer)
                } else {
                    // JDK8 or lower
                    if (dbbCleanerMethod != null && cleanMethod != null) {
                        // cleaner = ((DirectByteBuffer)buffer).cleaner()
                        val cleaner = dbbCleanerMethod.invoke(buffer)
                        // ((sun.misc.Cleaner)cleaner).clean()
                        cleanMethod.invoke(cleaner)
                    } else {
                        // for Android 5.1.1 try to call ((DirectByteBuffer)buffer).free()
                        if (JVMConstants.IS_ANDROID) {
                            dbbFreeMethod?.invoke(buffer)
                        }
                    }
                }
            }
        } catch (e: PrivilegedActionException) {
            throw RuntimeException(e)
        }
    }

    private fun getDirectByteBufferFreeMethod(): Method? {
        return getDirectByteBufferMethod("cleaner")
    }

    private fun getDirectByteBufferCleanerMethod(): Method? {
        return getDirectByteBufferMethod("cleaner")
    }

    private fun getDirectByteBufferMethod(name: String): Method? {
        return try {
            directByteBufferClass.getMethod(name).apply { isAccessible = true }
        } catch (t: Throwable) {
            null
        }
    }
}