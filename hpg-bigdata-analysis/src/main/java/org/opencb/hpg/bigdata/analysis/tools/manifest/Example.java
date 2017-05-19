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

import java.util.Map;

public class Example {

    private String name;
    private Map<String, Object> params;

    public Example() {

    }

    public Example(String name, Map<String, Object> params) {
        this.name = name;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Example{");
        sb.append("name='").append(name).append('\'');
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Example setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }
}
