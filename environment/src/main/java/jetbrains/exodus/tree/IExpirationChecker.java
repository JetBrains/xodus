/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.tree;

import jetbrains.exodus.log.Loggable;

public interface IExpirationChecker {

    public static final IExpirationChecker NONE = new IExpirationChecker() {

        @Override
        public boolean expired(Loggable loggable) {
            return false;
        }

        @Override
        public boolean expired(long startAddress, int length) {
            return false;
        }
    };

    /**
     * Is loggable expired?
     *
     * @param loggable loggable
     * @return true if it is known for sure that specified loggable is expired.
     */
    boolean expired(Loggable loggable);

    /**
     * Are all loggables in specified address range expired?
     *
     * @param startAddress left bound of address range
     * @param length       length of address range
     * @return true if it is known for sure that all loggables in specified address range are expired.
     */
    boolean expired(long startAddress, int length);
}
