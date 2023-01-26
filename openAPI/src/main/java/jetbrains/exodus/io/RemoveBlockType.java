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
package jetbrains.exodus.io;

import jetbrains.exodus.env.EnvironmentConfig;

/**
 * Type of action applied to the {@linkplain Block blocks} processed by database GC. GC moves accessible data
 * from old blocks to new ones, so old blocks can finally be removed using {@linkplain DataWriter#removeBlock(long, RemoveBlockType)}.
 * For debugging purposes, for supporting custom backup procedures and so on, blocks can also be renamed, not only removed.
 * Actual value of the second parameter passed to {@linkplain DataWriter#removeBlock(long, RemoveBlockType)} is controlled
 * by the {@linkplain EnvironmentConfig#GC_RENAME_FILES} setting.
 *
 * @see Block
 * @see DataWriter#removeBlock(long, RemoveBlockType)
 * @see EnvironmentConfig#getGcRenameFiles()
 * @see EnvironmentConfig#setGcRenameFiles(boolean)
 * @since 1.3.0
 */
public enum RemoveBlockType {
    Delete,
    Rename
}
