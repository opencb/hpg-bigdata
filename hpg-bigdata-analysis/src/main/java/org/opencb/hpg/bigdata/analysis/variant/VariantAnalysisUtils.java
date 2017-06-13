package org.opencb.hpg.bigdata.analysis.variant;

import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.io.IOException;
import java.util.List;

/**
 * Created by jtarraga on 13/06/17.
 */
public class VariantAnalysisUtils {

    /**
     * Add variant fitlers to the target variant dataset.
     *
     * @param filterOptions Filters to apply
     * @param vd            Target variant dataset
     * @throws IOException  Exception
     */
    public static void addVariantFilters(VariantFilterOptions filterOptions,
                                         VariantDataset vd) throws IOException {
        // ID list
        if (validList(filterOptions.getIdList())) {
            vd.idFilter(filterOptions.getIdList(), false);
        }

        // type
        if (validList(filterOptions.getTypeList())) {
            vd.typeFilter(filterOptions.getTypeList());
        }

        // query for biotype
        if (validList(filterOptions.getBiotypeList())) {
            vd.annotationFilter("biotype", filterOptions.getBiotypeList());
        }

        // query for study
        if (validList(filterOptions.getStudyList())) {
            vd.studyFilter("studyId", filterOptions.getStudyList());
        }

        // query for maf (study:cohort)
        if (validList(filterOptions.getMafList())) {
            vd.studyFilter("stats.maf", filterOptions.getMafList());
        }

        // query for mgf (study:cohort)
        if (validList(filterOptions.getMgfList())) {
            vd.studyFilter("stats.mgf", filterOptions.getMgfList());
        }

        // query for region
        if (validList(filterOptions.getRegionList())) {
            vd.regionFilter(filterOptions.getRegionList());
        }

        // query for consequence type (Sequence Ontology term names and accession codes)
        if (validList(filterOptions.getConsequenceTypeList())) {
            vd.annotationFilter("consequenceTypes.sequenceOntologyTerms", filterOptions.getConsequenceTypeList());
        }

        // query for consequence type (gene names)
        if (validList(filterOptions.getGeneList())) {
            vd.annotationFilter("consequenceTypes.geneName", filterOptions.getGeneList());
        }

        // query for clinvar (accession)
        if (validList(filterOptions.getClinvarList())) {
            vd.annotationFilter("variantTraitAssociation.clinvar.accession", filterOptions.getClinvarList());
        }

        // query for cosmic (mutation ID)
        if (validList(filterOptions.getCosmicList())) {
            vd.annotationFilter("variantTraitAssociation.cosmic.mutationId", filterOptions.getCosmicList());
        }

        // query for conservation (phastCons, phylop, gerp)
        if (validList(filterOptions.getConservScoreList())) {
            vd.annotationFilter("conservation", filterOptions.getConservScoreList());
        }

        // query for protein substitution scores (polyphen, sift)
        if (validList(filterOptions.getSubstScoreList())) {
            vd.annotationFilter("consequenceTypes.proteinVariantAnnotation.substitutionScores", filterOptions.getSubstScoreList());
        }

        // query for alternate population frequency (study:population)
        if (validList(filterOptions.getPfList())) {
            vd.annotationFilter("populationFrequencies.altAlleleFreq", filterOptions.getPfList());
        }

        // query for population minor allele frequency (study:population)
        if (validList(filterOptions.getPmafList())) {
            vd.annotationFilter("populationFrequencies.refAlleleFreq", filterOptions.getPmafList());
        }

        // query for sample genotypes
        // query for number of missing alleles (study:cohort)
        // query for number of missing genotypes (study:cohort)
    }

    /**
     * Sanity check.
     *
     * @param list  list to check
     * @return      Boolean
     */
    private static boolean validList(List list) {
        return (list != null && list.size() > 0);
    }
}

