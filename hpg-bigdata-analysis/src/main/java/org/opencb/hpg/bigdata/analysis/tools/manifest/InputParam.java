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

public class InputParam {

    private String name, description;
    private Object defaultValue;
    private boolean arity;

    private Type dataType;

    // This will mean that the actual option will not be written in the command line, just the result.
    // Example: <threads: 2>  Command-line construction: <2> and not <--threads 2> as we would expect by default.
    private boolean hidden;

    // Only applied for numeric dataTypes. It will be a vector containing the ranges of values. Example: [0, 1]"
    private List<Float> ranges;

    // There are options in some command lines that only accept some closed values. Example: [first-strand, second-strand, unstranded]
    private List<Object> acceptedValues;

    public InputParam() {
    }

    public InputParam(String name, String description, Object defaultValue, boolean arity, Type dataType, boolean hidden,
                      List<Float> ranges, List<Object> acceptedValues) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.arity = arity;
        this.dataType = dataType;
        this.hidden = hidden;
        this.ranges = ranges;
        this.acceptedValues = acceptedValues;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InputParam{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", defaultValue=").append(defaultValue);
        sb.append(", arity=").append(arity);
        sb.append(", dataType=").append(dataType);
        sb.append(", hidden=").append(hidden);
        sb.append(", ranges=").append(ranges);
        sb.append(", acceptedValues=").append(acceptedValues);
        sb.append('}');
        return sb.toString();
    }

    public enum Type {
        TEXT,
        NUMERIC,
        BOOLEAN,
        FILE,
        FOLDER
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public InputParam setDescription(String description) {
        this.description = description;
        return this;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public InputParam setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public boolean isArity() {
        return arity;
    }

    public InputParam setArity(boolean arity) {
        this.arity = arity;
        return this;
    }

    public Type getDataType() {
        return dataType;
    }

    public InputParam setDataType(Type dataType) {
        this.dataType = dataType;
        return this;
    }

    public boolean isHidden() {
        return hidden;
    }

    public InputParam setHidden(boolean hidden) {
        this.hidden = hidden;
        return this;
    }

    public List<Float> getRanges() {
        return ranges;
    }

    public InputParam setRanges(List<Float> ranges) {
        this.ranges = ranges;
        return this;
    }

    public List<Object> getAcceptedValues() {
        return acceptedValues;
    }

    public InputParam setAcceptedValues(List<Object> acceptedValues) {
        this.acceptedValues = acceptedValues;
        return this;
    }
}
