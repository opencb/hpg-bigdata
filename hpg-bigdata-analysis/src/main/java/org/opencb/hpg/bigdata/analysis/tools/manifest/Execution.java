/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.analysis.tools.manifest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Execution {

    private String id, name, executable, testCmd;

    /**
     * Flag indicating whether the parameters follow the POSIX command line standard.
     * If true, we will assume the parameters are preceded by '-' if it is a one letter parameter or '--' otherwise.
     */
    private boolean posix;

    private List<InputParam> params;

    public Execution() {
    }

    public Execution(String id, String name, String executable, String testCmd, boolean posix, List<InputParam> params) {
        this.id = id;
        this.name = name;
        this.executable = executable;
        this.testCmd = testCmd;
        this.posix = posix;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", executable='").append(executable).append('\'');
        sb.append(", testCmd='").append(testCmd).append('\'');
        sb.append(", posix=").append(posix);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Execution setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Execution setName(String name) {
        this.name = name;
        return this;
    }

    public String getExecutable() {
        return executable;
    }

    public Execution setExecutable(String executable) {
        this.executable = executable;
        return this;
    }

    public String getTestCmd() {
        return testCmd;
    }

    public Execution setTestCmd(String testCmd) {
        this.testCmd = testCmd;
        return this;
    }

    public boolean isPosix() {
        return posix;
    }

    public Execution setPosix(boolean posix) {
        this.posix = posix;
        return this;
    }

    public List<InputParam> getParams() {
        return params;
    }

    public Map<String, InputParam> getParamsAsMap() {
        Map<String, InputParam> mapParams = new HashMap<>();
        for (InputParam param : params) {
            mapParams.put(param.getName(), param);
        }
        return mapParams;
    }

    public Execution setParams(List<InputParam> params) {
        this.params = params;
        return this;
    }
}
