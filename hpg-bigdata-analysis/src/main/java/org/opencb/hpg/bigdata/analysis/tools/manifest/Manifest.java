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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class Manifest {

    private String id;
    private String name;
    private String version;
    private String description;
    private String website;
    private String publication;

    private Contact contact;
    private Git git;

    private ProgrammingLanguage language;
    private List<String> dependencies;
    private List<Execution> executions;

    private Settings settings;

    public Manifest() {
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static Manifest load(InputStream manifestStream) throws IOException {
        return load(manifestStream, "json");
    }

    private static Manifest load(InputStream manifestStream, String format) throws IOException {
        Manifest manifest;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                manifest = objectMapper.readValue(manifestStream, Manifest.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                manifest = objectMapper.readValue(manifestStream, Manifest.class);
                break;
        }
        return manifest;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Manifest{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", website='").append(website).append('\'');
        sb.append(", publication='").append(publication).append('\'');
        sb.append(", contact=").append(contact);
        sb.append(", git=").append(git);
        sb.append(", language=").append(language);
        sb.append(", dependencies=").append(dependencies);
        sb.append(", executions=").append(executions);
        sb.append(", settings=").append(settings);
        sb.append('}');
        return sb.toString();
    }

    public Contact getContact() {
        return contact;
    }

    public Manifest setContact(Contact contact) {
        this.contact = contact;
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

    public Git getGit() {
        return git;
    }

    public Manifest setGit(Git git) {
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

    public ProgrammingLanguage getLanguage() {
        return language;
    }

    public Manifest setLanguage(ProgrammingLanguage language) {
        this.language = language;
        return this;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    public Manifest setDependencies(List<String> dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public Manifest setExecutions(List<Execution> executions) {
        this.executions = executions;
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public Manifest setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }
}
