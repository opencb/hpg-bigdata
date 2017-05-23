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
    private String id, name, version, git, description, website, publication;
    private String separator;
    private ProgrammingLanguage language;
    private List<Execution> executions;

    public Manifest() {
    }

    public Manifest(Author author, String id, String name, String version, String git, String description, String website,
                    String publication, String separator, ProgrammingLanguage language, List<Execution> executions) {
        this.author = author;
        this.id = id;
        this.name = name;
        this.version = version;
        this.git = git;
        this.description = description;
        this.website = website;
        this.publication = publication;
        this.separator = separator;
        this.language = language;
        this.executions = executions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Manifest{");
        sb.append("author=").append(author);
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", git='").append(git).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", website='").append(website).append('\'');
        sb.append(", publication='").append(publication).append('\'');
        sb.append(", separator='").append(separator).append('\'');
        sb.append(", language=").append(language);
        sb.append(", executions=").append(executions);
        sb.append('}');
        return sb.toString();
    }

    public Author getAuthor() {
        return author;
    }

    public Manifest setAuthor(Author author) {
        this.author = author;
        return this;
    }

    public String getId() {
        return id;
    }

    public Manifest setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Manifest setName(String name) {
        this.name = name;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Manifest setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getGit() {
        return git;
    }

    public Manifest setGit(String git) {
        this.git = git;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Manifest setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getWebsite() {
        return website;
    }

    public Manifest setWebsite(String website) {
        this.website = website;
        return this;
    }

    public String getPublication() {
        return publication;
    }

    public Manifest setPublication(String publication) {
        this.publication = publication;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public Manifest setSeparator(String separator) {
        this.separator = separator;
        return this;
    }

    public ProgrammingLanguage getLanguage() {
        return language;
    }

    public Manifest setLanguage(ProgrammingLanguage language) {
        this.language = language;
        return this;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public Manifest setExecutions(List<Execution> executions) {
        this.executions = executions;
        return this;
    }
}
