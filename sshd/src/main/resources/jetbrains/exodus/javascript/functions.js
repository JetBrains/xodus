/*
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
function print(o) {
    interop.print(o);
}

function println(o) {
    interop.println(o);
}

function load(filename) {
    interop.load(filename);
}

function getEnv() {
    return interop.getEnv()
}

function getStore() {
    return interop.getStore()
}

function iter(iterable, f) {
    var iter = iterable.iterator();
    while (iter.hasNext()) {
        var item = iter.next();
        f(item);
    }
}