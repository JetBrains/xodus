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
var bindings = Packages.jetbrains.exodus.bindings;
var stringBinding = bindings.StringBinding;
var integerBinding = bindings.IntegerBinding;
var longBinding = bindings.LongBinding;
var doubleBinding = bindings.DoubleBinding

function stringToEntry(s) {
    return stringBinding.stringToEntry(s);
}

function entryToString(e) {
    return stringBinding.entryToString(e);
}

function intToEntry(i) {
    return integerBinding.intToEntry(i);
}

function entryToInt(e) {
    return integerBinding.entryToInt(e);
}

function intToCompressedEntry(i) {
    return integerBinding.intToCompressedEntry(i);
}

function compressedEntryToInt(e) {
    return integerBinding.compressedEntryToInt(e);
}

function signedIntToCompressedEntry(i) {
    return integerBinding.signedIntToCompressedEntry(i);
}

function compressedEntryToSignedInt(e) {
    return integerBinding.compressedEntryToSignedInt(e);
}

function longToEntry(l) {
    return longBinding.longToEntry(l);
}

function entryToLong(e) {
    return longBinding.entryToLong(e);
}

function longToCompressedEntry(l) {
    return longBinding.longToCompressedEntry(l);
}

function compressedEntryToLong(e) {
    return longBinding.compressedEntryToLong(e);
}

function doubleToEntry(d) {
    return doubleBinding.doubleToEntry(d);
}

function entryToDouble(e) {
    return doubleBinding.entryToDouble(e);
}