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

public class Manifest {

    private Author author;
    private String version, id, name, description, website, publication, separator;
    private Icon icon;
    private List<Option> globalParams;
    private List<Execution> executions;

    public Manifest() {
    }

    public Manifest(Author author, String version, String id, String name, String description, String website, String publication,
                    String separator, Icon icon, List<Option> globalParams, List<Execution> executions) {
        this.author = author;
        this.version = version;
        this.id = id;
        this.name = name;
        this.description = description;
        this.website = website;
        this.publication = publication;
        this.separator = separator;
        this.icon = icon;
        this.globalParams = globalParams;
        this.executions = executions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Manifest{");
        sb.append("author=").append(author);
        sb.append(", version='").append(version).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", website='").append(website).append('\'');
        sb.append(", publication='").append(publication).append('\'');
        sb.append(", separator='").append(separator).append('\'');
        sb.append(", icon=").append(icon);
        sb.append(", globalParams=").append(globalParams);
        sb.append(", executions=").append(executions);
        sb.append('}');
        return sb.toString();
    }

    public Author getAuthor() {
        return author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public void setDescription(String description) {
        this.description = description;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getPublication() {
        return publication;
    }

    public void setPublication(String publication) {
        this.publication = publication;
    }

    public Icon getIcon() {
        return icon;
    }

    public void setIcon(Icon icon) {
        this.icon = icon;
    }

    public List<Option> getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(List<Option> globalParams) {
        this.globalParams = globalParams;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public void setExecutions(List<Execution> executions) {
        this.executions = executions;
    }

    public String getSeparator() {
        return separator;
    }

    public Manifest setSeparator(String separator) {
        this.separator = separator;
        return this;
    }
}
