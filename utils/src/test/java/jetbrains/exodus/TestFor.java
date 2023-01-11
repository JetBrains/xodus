/**
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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation binds production class with its tests
 *
 * @author eugene.petrenko@gmail.com
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface TestFor {

    /**
     * Binds implementation class with a test to be able to find the class by the test.
     */
    Class[] testForClass() default {};

    /**
     * Binds test with issues.
     *
     * @return issue IDs
     */
    @NotNull String[] issues() default {};

    /**
     * Binds test with a single issue.
     *
     * @return issue ID
     */
    @NotNull String issue() default "";

    /**
     * Binds test with a question on Stack Overflow.
     *
     * @return URL of a question on Stack Overflow
     */
    @NotNull String question() default "";
}