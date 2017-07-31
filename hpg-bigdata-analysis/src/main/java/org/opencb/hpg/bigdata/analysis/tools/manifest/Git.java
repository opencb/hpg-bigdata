package org.opencb.hpg.bigdata.analysis.tools.manifest;

/**
 * Created by pfurio on 31/05/17.
 */
public class Git {

    private String url;
    private String commit;
    private String tag;

    public Git() {
    }

    public Git(String url, String commit, String tag) {
        this.url = url;
        this.commit = commit;
        this.tag = tag;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Git{");
        sb.append("url='").append(url).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public Git setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public Git setCommit(String commit) {
        this.commit = commit;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public Git setTag(String tag) {
        this.tag = tag;
        return this;
    }
}
