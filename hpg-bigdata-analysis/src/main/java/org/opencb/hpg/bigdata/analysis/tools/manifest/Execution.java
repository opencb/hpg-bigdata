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

import java.util.List;

public class Execution {

    private String id, name, executable, testCmd;
    private List<String> input;
    private List<String> output;
    private List<InputParam> params;
    private List<ConfigAttr> configAttr;
    private List<Example> examples;

    public Execution() {
    }

    public Execution(String id, String name, String executable, String testCmd, List<String> input, List<String> output,
                     List<InputParam> params, List<ConfigAttr> configAttr, List<Example> examples) {
        this.id = id;
        this.name = name;
        this.executable = executable;
        this.testCmd = testCmd;
        this.input = input;
        this.output = output;
        this.params = params;
        this.configAttr = configAttr;
        this.examples = examples;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", executable='").append(executable).append('\'');
        sb.append(", testCmd='").append(testCmd).append('\'');
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", params=").append(params);
        sb.append(", configAttr=").append(configAttr);
        sb.append(", examples=").append(examples);
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

    public List<String> getInput() {
        return input;
    }

    public Execution setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public List<String> getOutput() {
        return output;
    }

    public Execution setOutput(List<String> output) {
        this.output = output;
        return this;
    }

    public List<InputParam> getParams() {
        return params;
    }

    public Execution setParams(List<InputParam> params) {
        this.params = params;
        return this;
    }

    public List<ConfigAttr> getConfigAttr() {
        return configAttr;
    }

    public Execution setConfigAttr(List<ConfigAttr> configAttr) {
        this.configAttr = configAttr;
        return this;
    }

    public List<Example> getExamples() {
        return examples;
    }

    public Execution setExamples(List<Example> examples) {
        this.examples = examples;
        return this;
    }
}
