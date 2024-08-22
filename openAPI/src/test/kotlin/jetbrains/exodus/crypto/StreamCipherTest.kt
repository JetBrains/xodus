/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

import jetbrains.exodus.util.LightOutputStream
import org.junit.Assert
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.InputStream

private val KEY =
        byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
private val IV = 314159262718281828L

open class StreamCipherTest {

    @Test
    fun testTrivialCipher() {
        val cipherInputStream = makeInput() decryptBy { createCipher().init() }
        Assert.assertEquals(RENAT_GILFANOV, cipherInputStream.reader().readText())
    }

    @Test
    fun testTrivialCipherBlockWise() {
        val cipherInputStream = makeInput().buffered(32) decryptBy { createCipher().init() }
        Assert.assertEquals(RENAT_GILFANOV, String(cipherInputStream.readBytesBlockWise(64)))
    }

    private fun makeInput(): ByteArrayInputStream {
        val baseOutputStream = LightOutputStream()
        val cipherOutputStream = baseOutputStream encryptBy createCipher().init()
        cipherOutputStream.writer().use {
            it.write(RENAT_GILFANOV)
        }
        return ByteArrayInputStream(baseOutputStream.bufferBytes, 0, baseOutputStream.size())
    }

    private fun InputStream.readBytesBlockWise(block: Int): ByteArray {
        var offset = 0
        var remaining = available()
        val result = ByteArray(remaining)
        while (remaining > 0) {
            val read = read(result, offset, minOf(block, remaining))
            if (read < 0) break
            remaining -= read
            offset += read
        }
        return if (remaining == 0) result else result.copyOf(offset)
    }

    open fun createCipher(): StreamCipher = TrivialStreamCipher()
}

private fun StreamCipher.init() = this.with(KEY, IV)

val RENAT_GILFANOV = """
           �?едавно прочёл в газете: в результате множе�?тва наблюдений
           над жизнью и разнообразными формами привидений
           доказано, что привидени�? �?ти — не про�?то тени,
           а фотографии реально�?ти, которые �?делали �?тены.

           В веках моих — прожилки, в пепельнице — окурки.
           Мы живём, чтоб о�?тавить �?вой профиль на штукатурке.
           Чтоб ви�?ел он навроде кра�?ивой индей�?кой ма�?ки,
           и �?олнце в его морщинах под вечер �?гущало кра�?ки.

           Старые люди — �?таканы, до дна недопиты.
           О�?татки — мутны, волокни�?ты, и�?порчены, �?довиты.
           Лица �?тарых людей молодых людей заражают
           тем, что резкие их морщины, извива�?�?�?, выражают.

           Лицо молодого — пару�?, �?тарого же — папиру�?,
           кажда�? буква которого — �?мертельный виру�?.
           С �?тих позиций, в общем, и боль мо�? не�?уразна.
           Я �?очин�?ю музыку, а музыка — не заразна.

           Музыка не �?по�?обна — и в �?том она не чета мне -
           заполонить про�?тран�?тво �?об�?твенными чертами.
           Музыка — �?то зеркало, где амальгама — нежно�?ть.
           И при �?том она не ворует чужую внешно�?ть.

           «Пам�?ть �?тановит�?�? гладкой, �?тена — р�?бою».
           Так �?казал мне один �?тарик, вы�?охшею губою
           под грохот токарных �?танков му�?ол�? патрон «Казбека».
           «Стена �?охранит то, что �?терло�?ь из пам�?ти человека».""".trimIndent()
