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
package jetbrains.exodus

import kotlin.reflect.KClass

/**
 * This annotation binds production class with its tests
 *
 * @author eugene.petrenko@gmail.com
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY_GETTER,
    AnnotationTarget.PROPERTY_SETTER
)
annotation class TestFor(
    /**
     * Binds implementation class with a test to be able to find the class by the test.
     */
    val testForClass: Array<KClass<*>> = [],
    /**
     * Binds test with issues.
     *
     * @return issue IDs
     */
    val issues: Array<String> = [],
    /**
     * Binds test with a single issue.
     *
     * @return issue ID
     */
    val issue: String = "",
    /**
     * Binds test with a question on Stack Overflow.
     *
     * @return URL of a question on Stack Overflow
     */
    val question: String = ""
)