package org.opencb.hpg.bigdata.analysis.variant.adaptors;

import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;

import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 09/06/17.
 */
public class PlinkAdaptor extends AnalysisExecutor {
    private String inFilename;
    private String metaFilename;
    private String outDirname;

    private int splitSize;
    private List<String> plinkParams;
    private Map<String, String> filterOptions;

    public PlinkAdaptor(String inFilename, String metaFilename, String outDirname) {
        this.inFilename = inFilename;
        this.metaFilename = inFilename + AnalysisExecutor.metadataExtension;
        this.outDirname = outDirname;
    }

    @Override
    public void execute() throws AnalysisExecutorException {
        System.out.println("plink params = " + plinkParams);
        System.out.println("filter options = " + filterOptions);
    }

    public String getInFilename() {
        return inFilename;
    }

    public void setInFilename(String inFilename) {
        this.inFilename = inFilename;
    }

    public String getMetaFilename() {
        return metaFilename;
    }

    public void setMetaFilename(String metaFilename) {
        this.metaFilename = metaFilename;
    }

    public String getOutDirname() {
        return outDirname;
    }

    public void setOutDirname(String outDirname) {
        this.outDirname = outDirname;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    public List<String> getPlinkParams() {
        return plinkParams;
    }

    public void setPlinkParams(List<String> plinkParams) {
        this.plinkParams = plinkParams;
    }

    public Map<String, String> getFilterOptions() {
        return filterOptions;
    }

    public void setFilterOptions(Map<String, String> filterOptions) {
        this.filterOptions = filterOptions;
    }
}
