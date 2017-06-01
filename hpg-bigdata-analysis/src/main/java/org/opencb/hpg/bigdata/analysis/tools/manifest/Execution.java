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

    private String id;
    private String bin;
    private String description;
    private String usage;

    private List<Param> params;

    public Execution() {
    }

    public Execution(String id, String bin, String description, String usage, List<Param> params) {
        this.id = id;
        this.bin = bin;
        this.description = description;
        this.usage = usage;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", bin='").append(bin).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", usage='").append(usage).append('\'');
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

    public String getBin() {
        return bin;
    }

    public Execution setBin(String bin) {
        this.bin = bin;
        return this;
    }

    public List<Param> getParams() {
        return params;
    }

    public Map<String, Param> getParamsAsMap() {
        Map<String, Param> mapParams = new HashMap<>();
        for (Param param : params) {
            mapParams.put(param.getName(), param);
        }
        return mapParams;
    }

    public String getDescription() {
        return description;
    }

    public Execution setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getUsage() {
        return usage;
    }

    public Execution setUsage(String usage) {
        this.usage = usage;
        return this;
    }

    public Execution setParams(List<Param> params) {
        this.params = params;
        return this;
    }
}
