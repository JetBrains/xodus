/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.testutil

import mu.KLogging
import java.time.Duration

typealias Condition = () -> Boolean
typealias Assertion = () -> Unit

private const val SLEEP_INTERNAL_MILLIS = 100L

/**
 * This class waits actively for a specified condition to be met using Thread.sleep mechanism.
 *
 * Condition is checked in optimistic way, meaning that even if timeout is reached there is a possibility
 * that condition is met in the same time and method will return without throwing an exception.
 */
class BlockingAwaiter(val timeout: Duration) {

    companion object : KLogging()

    private var sleepInterval = SLEEP_INTERNAL_MILLIS

    private var name: String? = null

    fun withName(name: String?): BlockingAwaiter {
        this.name = name
        return this
    }

    fun untilAssertion(assertion: Assertion) {
        var assertionError: AssertionError? = null
        val conditionMet = this.await {
            try {
                assertion()
                true
            } catch (e: AssertionError) {
                assertionError = e
                false
            }
        }
        if (!conditionMet) {
            assertionError!!.let {
                val additionalInfo = name?.let { ": $name" } ?: ""
                throw AssertionError(
                    "Assertion error after waiting for ${timeout.toHumanString()}$additionalInfo",
                    it
                )
            }
        }
    }

    private fun await(condition: Condition): Boolean {
        // https://stackoverflow.com/questions/1770010/how-do-i-measure-time-elapsed-in-java/1776053#1776053
        val start = System.nanoTime()
        while (true) {
            val conditionMet = condition()

            val now = System.nanoTime()
            val elapsed = Duration.ofNanos(now - start)

            if (conditionMet) {
                logger.info("Condition $name met after ${elapsed.toHumanString()}")
                return true
            } else if (elapsed > timeout) {
                return false
            } else {
                Thread.sleep(this.sleepInterval)
            }
        }
    }
}

// 10 seconds proved to be enough empirically
private val DEFAULT_TIMEOUT = Duration.ofSeconds(10)

fun awaitAssertion(name: String? = null, timeout: Duration = DEFAULT_TIMEOUT, assertion: Assertion) {
    BlockingAwaiter(timeout)
        .withName(name)
        .untilAssertion(assertion)
}

fun eventually(name: String? = null, timeout: Duration = DEFAULT_TIMEOUT, assertion: Assertion) {
    awaitAssertion(name, timeout, assertion)
}