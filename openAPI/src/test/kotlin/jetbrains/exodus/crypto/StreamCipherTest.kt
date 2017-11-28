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
package jetbrains.exodus.crypto

import jetbrains.exodus.util.LightOutputStream
import org.junit.Assert
import org.junit.Test
import java.io.ByteArrayInputStream
import kotlin.experimental.xor

open class StreamCipherTest {

    @Test
    fun testTrivialCipher() {
        val baseOutputStream = LightOutputStream()
        val cipherOutputStream = baseOutputStream encryptBy createCipher().init()
        cipherOutputStream.writer().use {
            it.write(RENAT_GILFANOV)
        }
        val baseInputStream = ByteArrayInputStream(baseOutputStream.bufferBytes, 0, baseOutputStream.size())
        val cipherInputStream = baseInputStream decryptBy createCipher().init()
        Assert.assertEquals(RENAT_GILFANOV, cipherInputStream.reader().readText())
    }

    open fun createCipher(): StreamCipher = TrivialStreamCipher()
}

private fun StreamCipher.init() = this.with(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), 314159262718281828L)

private const val RENAT_GILFANOV =
        "Недавно прочёл в газете: в результате множества наблюдений\n" +
                "над жизнью и разнообразными формами привидений\n" +
                "доказано, что привидения эти — не просто тени,\n" +
                "а фотографии реальности, которые сделали стены.\n" +
                "\n" +
                "В веках моих — прожилки, в пепельнице — окурки.\n" +
                "Мы живём, чтоб оставить свой профиль на штукатурке.\n" +
                "Чтоб висел он навроде красивой индейской маски,\n" +
                "и солнце в его морщинах под вечер сгущало краски.\n" +
                "\n" +
                "Старые люди — стаканы, до дна недопиты. \n" +
                "Остатки — мутны, волокнисты, испорчены, ядовиты.\n" +
                "Лица старых людей молодых людей заражают\n" +
                "тем, что резкие их морщины, извиваяся, выражают.\n" +
                "\n" +
                "Лицо молодого — парус, старого же — папирус,\n" +
                "каждая буква которого — смертельный вирус.\n" +
                "С этих позиций, в общем, и боль моя несуразна.\n" +
                "Я сочиняю музыку, а музыка — не заразна.\n" +
                "\n" +
                "Музыка не способна — и в этом она не чета мне -\n" +
                "заполонить пространство собственными чертами.\n" +
                "Музыка — это зеркало, где амальгама — нежность.\n" +
                "И при этом она не ворует чужую внешность.\n" +
                "\n" +
                "«Память становится гладкой, стена — рябою».\n" +
                "Так сказал мне один старик, высохшею губою\n" +
                "под грохот токарных станков мусоля патрон «Казбека».\n" +
                "«Стена сохранит то, что стерлось из памяти человека»."

private class TrivialStreamCipher : StreamCipher {

    private lateinit var key: ByteArray
    private var iv: Int = 0

    override fun init(key: ByteArray, iv: Long) {
        this.key = key
        this.iv = iv.toInt() xor (iv shr 32).toInt()
    }

    override fun crypt(b: Byte): Byte {
        return b xor key[(iv++ and 0x7fffffff).rem(key.size)]
    }
}