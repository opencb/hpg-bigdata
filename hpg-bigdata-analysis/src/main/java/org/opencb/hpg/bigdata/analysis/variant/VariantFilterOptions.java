package org.opencb.hpg.bigdata.analysis.variant;

import org.opencb.biodata.models.core.Region;

import java.util.List;

/**
 * Created by jtarraga on 13/06/17.
 */
public class VariantFilterOptions {

    // Filter ID
    private List<String> idList;

    // Filter type
    private List<String> typeList;

    // Filter study
    private List<String> studyList;

    // Filter biotype
    private List<String> biotypeList;

    // Filter regions
    private List<Region> regionList;

    // Filter Minor Allele Frequency (maf), study_name::cohort_name[<|>|<=|>=|==|!=]value
    // e.g.: 1000g::all>0.4
    private List<String> mafList;

    // Filter Minor Genotype Frequency (mgf), study_name::cohort_name[<|>|<=|>=|==|!=]value
    // e.g.: 1000g::all>0.18198
    private List<String> mgfList;

    // Filter consequence type, Sequence Ontology term names or accession codes
    // e.g.: transgenic insertion,SO:32234,SO:00124
    private List<String> consequenceTypeList;

    // Filter gene
    // e.g.: BIN3,ZNF517
    private List<String> geneList;

    // Filter clinvar
    private List<String> clinvarList;

    // Filter cosmic
    private List<String> cosmicList;

    // Filter conservation scores (phastCons, phylop, gerp)
    // e.g.: phylop<0.3,phastCons<0.1
    private List<String> conservScoreList;

    // Filter protein substitution scores
    // e.g.: polyphen>0.3,sift>0.6
    private List<String> substScoreList;

    // Filter alternate population frequency of a given study, study_name::population_name[<|>|<=|>=|==|!=]frequency_value
    // e.g.: 1000g::CEU<0.4
    private List<String> pfList;

    // Filter population minor allele frequency of a given study: study_name:: population_name[<|>|<=|>=|==|!=]frequency_value
    // e.g.: 1000g::PJL<=0.25
    private List<String> pmafList;

    //public String samples;

    public List<String> getIdList() {
        return idList;
    }

    public void setIdList(List<String> idList) {
        this.idList = idList;
    }

    public List<String> getTypeList() {
        return typeList;
    }

    public void setTypeList(List<String> typeList) {
        this.typeList = typeList;
    }

    public List<String> getStudyList() {
        return studyList;
    }

    public void setStudyList(List<String> studyList) {
        this.studyList = studyList;
    }

    public List<String> getBiotypeList() {
        return biotypeList;
    }

    public void setBiotypeList(List<String> biotypeList) {
        this.biotypeList = biotypeList;
    }

    public List<Region> getRegionList() {
        return regionList;
    }

    public void setRegionList(List<Region> regionList) {
        this.regionList = regionList;
    }

    public List<String> getMafList() {
        return mafList;
    }

    public void setMafList(List<String> mafList) {
        this.mafList = mafList;
    }

    public List<String> getMgfList() {
        return mgfList;
    }

    public void setMgfList(List<String> mgfList) {
        this.mgfList = mgfList;
    }

    public List<String> getConsequenceTypeList() {
        return consequenceTypeList;
    }

    public void setConsequenceTypeList(List<String> consequenceTypeList) {
        this.consequenceTypeList = consequenceTypeList;
    }

    public List<String> getGeneList() {
        return geneList;
    }

    public void setGeneList(List<String> geneList) {
        this.geneList = geneList;
    }

    public List<String> getClinvarList() {
        return clinvarList;
    }

    public void setClinvarList(List<String> clinvarList) {
        this.clinvarList = clinvarList;
    }

    public List<String> getCosmicList() {
        return cosmicList;
    }

    public void setCosmicList(List<String> cosmicList) {
        this.cosmicList = cosmicList;
    }

    public List<String> getConservScoreList() {
        return conservScoreList;
    }

    public void setConservScoreList(List<String> conservScoreList) {
        this.conservScoreList = conservScoreList;
    }

    public List<String> getSubstScoreList() {
        return substScoreList;
    }

    public void setSubstScoreList(List<String> substScoreList) {
        this.substScoreList = substScoreList;
    }

    public List<String> getPfList() {
        return pfList;
    }

    public void setPfList(List<String> pfList) {
        this.pfList = pfList;
    }

    public List<String> getPmafList() {
        return pmafList;
    }

    public void setPmafList(List<String> pmafList) {
        this.pmafList = pmafList;
    }
}
