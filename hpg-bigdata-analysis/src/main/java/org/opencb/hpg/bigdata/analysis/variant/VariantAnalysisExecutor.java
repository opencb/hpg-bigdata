package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.sql.Row;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutor;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantAnalysisExecutor extends AnalysisExecutor {

    protected String studyName;

    protected VariantDataset filter(Path path, Map<String, String> filterOptions) throws Exception {
        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(path.toString());
        vd.createOrReplaceTempView("vcf");

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT id, chromosome, start, study.samplesData FROM vcf ");
        sb.append("LATERAL VIEW explode(studies) act as study ");
        //sb.append("LATERAL VIEW explode(study.samplesData[0]) act as samplesData ");
        sb.append("WHERE study.studyId = '" + datasetName + "' ");

        return vd;
    }

    protected List<Row> getRows(Path path, VariantFilterOptions filterOptions) throws Exception {
        VariantDataset vd = new VariantDataset(sparkSession);

        vd.load(path.toString());
        vd.createOrReplaceTempView("vcf");
        VariantAnalysisUtils.addVariantFilters(filterOptions, vd);

        String sql = vd.getSql().replace(" * ", " id, chromosome, start, study.samplesData ")
                .replace(" vcf ", " vcf LATERAL VIEW explode(studies) act as study ");
        System.out.println(">>>>>>  SQL = " + sql);
        return vd.sqlContext().sql(sql).collectAsList();
        //Dataset<Row> dr = vd.sqlContext().sql(vd.getSql());

        //StringBuilder sb = new StringBuilder();
        //sb.append("SELECT id, chromosome, start, study.samplesData FROM vcf2 ");
        //sb.append("LATERAL VIEW explode(studies) act as study ");
        //dr.createOrReplaceTempView("vcf2");

        //return dr.sqlContext().sql(sb.toString()).collectAsList();
    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }
}
