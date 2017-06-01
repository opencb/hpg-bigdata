package org.opencb.hpg.bigdata.analysis.tools.manifest;


import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by pfurio on 23/05/17.
 */
public class ManifestTest {

    @Test
    public void testDefault() {
        Manifest manifest = new Manifest();

        manifest.setContact(new Contact("John", "john@email.org"));
        manifest.setDescription("Manifest description");
        manifest.setGit(new Git("http://github.com", "Git commit", "tag"));
        manifest.setId("Tool id");
        manifest.setName("Tool name");
        manifest.setVersion("v1");
        manifest.setPublication("URL to publication");
        manifest.setLanguage(new ProgrammingLanguage("bash", "v1"));
        manifest.setDependencies(Arrays.asList("Java", "htsjdk"));
        manifest.setSettings(new Settings(" ", "setEnv.sh", true));
        manifest.setWebsite("http://www.mywebsite.org");

        Execution execution1 = new Execution("exec1", "tool exec1", "Description", "usage", Arrays.asList(
                new Param("input", "Input blabla", null, false, true, false, false, 0, false, null, Param.Type.STRING),
                new Param("output", "Output blabla", null, false, true, true, false, 0, false, null, Param.Type.STRING)
        ));

        Execution execution2 = new Execution("exec2", "tool exec2", "Description", "usage", Arrays.asList(
                new Param("input", "Input blabla", null, false, true, false, false, 0, false, null, Param.Type.STRING),
                new Param("output", "Output blabla", null, false, true, true, false, 0, false, null, Param.Type.STRING)
        ));

        manifest.setExecutions(Arrays.asList(execution1, execution2));

        try {
            manifest.serialize(new FileOutputStream("/tmp/manifest-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {
        Manifest manifest = Manifest.load(getClass().getResource("/manifest-test.yml").openStream());
        System.out.println("Manifest = " + manifest);
    }

}