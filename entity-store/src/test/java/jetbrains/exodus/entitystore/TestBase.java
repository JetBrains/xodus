/**
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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.util.CompressBackupUtil;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public abstract class TestBase extends TestCase {

    protected static Logger logger = LoggerFactory.getLogger(TestBase.class);

    protected TestBase() {
    }

    @Override
    protected void runTest() throws Throwable {
        boolean failed = false;
        try {
            super.runTest();
        } catch (Throwable throwable) {
            failed = true;
            throw throwable;
        } finally {
            try {
                String methodName = getMethodName();
                Method method = getClass().getMethod(methodName, (Class<?>[]) null);
                if (needsArtifactBackup(method, failed)) {
                    storeArtifacts(methodName);
                }
            } catch (NoSuchMethodException e) {
            }
        }
    }

    private String getMethodName() throws IllegalAccessException, NoSuchFieldException {
        Field testNameField = TestCase.class.getDeclaredField("fName");
        testNameField.setAccessible(true);
        return (String) testNameField.get(this);
    }

    private boolean needsArtifactBackup(Method method, boolean isFailure) {
        StoreArtifactsPolicy classLevel = getAnnotationValue(getClass());
        StoreArtifactsPolicy methodLevel = getAnnotationValue(method);
        StoreArtifactsPolicy summ = methodLevel != null ? methodLevel : classLevel;
        return summ != null && (summ != StoreArtifactsPolicy.ON_FAILURE_ONLY || isFailure);
    }

    private StoreArtifactsPolicy getAnnotationValue(AnnotatedElement element) {
        return element.getAnnotation(StoreArtifacts.class) == null ?
                null : getClass().getAnnotation(StoreArtifacts.class).storagePolicy();
    }

    protected void storeArtifacts(String methodName) throws Exception {
        File backupDestDir = new File(getArtifactsPath() + getClass().getSimpleName() + File.pathSeparatorChar + methodName);
        if (!(backupDestDir.isDirectory()) && !(backupDestDir.mkdirs())) {
            throw new IOException("Failed to create " + backupDestDir.getAbsolutePath());
        }
        File[] files = getFilesToBackup();
        for (File file : files) {
            if (!(file.exists())) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Can not find file while storing test artifacts: " + file.getAbsolutePath());
                }
                continue;
            }
            CompressBackupUtil.tar(file, new File(backupDestDir, file.getName() + System.currentTimeMillis() + ".tar.bz"));
        }
    }

    protected abstract File[] getFilesToBackup();

    protected abstract String getArtifactsPath();

}
