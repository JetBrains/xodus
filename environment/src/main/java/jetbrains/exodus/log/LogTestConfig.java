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
package jetbrains.exodus.log;

public class LogTestConfig {

    private long maxHighAddress;
    private boolean settingHighAddressDenied;

    /**
     * If maxHighAddress is set to a non-negative value, no loggable with address GE to it will be written.
     */
    public long getMaxHighAddress() {
        return maxHighAddress;
    }

    public void setMaxHighAddress(final long maxHighAddress) {
        this.maxHighAddress = maxHighAddress;
    }

    /**
     * If settingHighAddressDenied is true, attempt to modify log high address with Log.setHighAddress() will fail.
     */
    public boolean isSettingHighAddressDenied() {
        return settingHighAddressDenied;
    }

    public void setSettingHighAddressDenied(final boolean settingHighAddressDenied) {
        this.settingHighAddressDenied = settingHighAddressDenied;
    }
}
