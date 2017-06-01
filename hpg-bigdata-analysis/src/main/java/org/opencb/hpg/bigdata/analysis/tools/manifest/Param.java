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

public class Param {

    private String name, description;
    private Object defaultValue;

    /**
     * Field indicating that the parameter is a flag and it is not expecting any value.
     */
    private boolean flag;

    /**
     * Flag indicating whether the parameter is a mandatory parameter.
     */
    private boolean required;

    /**
     * Boolean indicating if the parameter is an output parameter. Output parameters can only be of type FILE or FOLDER.
     * If redirection is set, then type must be FILE.
     */
    private boolean output;

    /**
     * Flag only valid for output parameters. This is set only for tools that redirects the output to one file and do not take
     * output files or folders as parameters.
     */
    private boolean redirection;

    /**
     * This will be used to place params the following way: bin {pos<=0} {pos<=0} {pos<=0} {pos=1} {pos=2} ...
     */
    private int position;

    /**
     * Flag indicating the actual parameter name will not be written in the command line, just the value.
     * Example: <threads: 2>  Command-line construction: <2> and not <--threads 2> as we would expect by default.
     */
    private boolean hidden;
    /**
     * List of accepted values. Only expected for parameters that only accepts some concrete values.
     * Example: [first-strand, second-strand, unstranded].
     *          Ranges: ["[-5,-1]"]
     */
    private List<String> acceptedValues;

    private Type dataType;

    public enum Type {
        STRING,
        NUMERIC,
        BOOLEAN,
        FILE,
        FOLDER
    }

    public Param() {
    }

    public Param(String name, String description, Object defaultValue, boolean flag, boolean required, boolean output, boolean redirection,
                 int position, boolean hidden, List<String> acceptedValues, Type dataType) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.flag = flag;
        this.required = required;
        this.output = output;
        this.redirection = redirection;
        this.position = position;
        this.hidden = hidden;
        this.acceptedValues = acceptedValues;
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Param{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", defaultValue=").append(defaultValue);
        sb.append(", flag=").append(flag);
        sb.append(", required=").append(required);
        sb.append(", output=").append(output);
        sb.append(", redirection=").append(redirection);
        sb.append(", position=").append(position);
        sb.append(", hidden=").append(hidden);
        sb.append(", acceptedValues=").append(acceptedValues);
        sb.append(", dataType=").append(dataType);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Param setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Param setDescription(String description) {
        this.description = description;
        return this;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Param setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public boolean isFlag() {
        return flag;
    }

    public Param setFlag(boolean flag) {
        this.flag = flag;
        return this;
    }

    public boolean isRequired() {
        return required;
    }

    public Param setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public boolean isOutput() {
        return output;
    }

    public Param setOutput(boolean output) {
        this.output = output;
        return this;
    }

    public boolean isRedirection() {
        return redirection;
    }

    public Param setRedirection(boolean redirection) {
        this.redirection = redirection;
        return this;
    }

    public int getPosition() {
        return position;
    }

    public Param setPosition(int position) {
        this.position = position;
        return this;
    }

    public boolean isHidden() {
        return hidden;
    }

    public Param setHidden(boolean hidden) {
        this.hidden = hidden;
        return this;
    }

    public List<String> getAcceptedValues() {
        return acceptedValues;
    }

    public Param setAcceptedValues(List<String> acceptedValues) {
        this.acceptedValues = acceptedValues;
        return this;
    }

    public Type getDataType() {
        return dataType;
    }

    public Param setDataType(Type dataType) {
        this.dataType = dataType;
        return this;
    }
}
