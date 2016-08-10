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

package org.opencb.hpg.bigdata.core.lib;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by imedina on 09/08/16.
 */
public class VariantParseQuery {

    private Set<String> explodes;
    private List<String> filters;

    private StringBuilder sqlQueryString;

    public VariantParseQuery() {
        explodes = new LinkedHashSet<>();
        filters = new ArrayList<>();

        sqlQueryString = new StringBuilder();
    }


    public String parse(Query query, QueryOptions queryOptions, String viewName) {

        Set<String> keySet = query.keySet();
        for (String key : keySet) {
            String[] fields = key.split("\\.");

            // First we check if there is any ...
            if (fields.length == 0) {
                return null;
            }

            switch (fields[0]) {
                case "id":
                case "chromosome":
                case "type":
                    filters.add(fields[0] + " = '" + query.get(key) + "'");
                    break;
                case "start":
                case "end":
                case "length":
                    filters.add(fields[0] + query.get(key));
                    break;
                case "names":
                    filters.add("array_contains(annotation.hgvs, '" + fields[fields.length - 1] + "')");
                    break;
                case "studies":
                    processStudyQuery(fields, query.get(key));
                    break;
                case "annotation":
                    processAnnotationQuery(fields, query.get(key));
                    break;
                default:
                    break;
            }
        }

        // Build the SQL string from the processed query using explodes and filters
        if (queryOptions != null && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.INCLUDE))) {
            sqlQueryString.append("SELECT " + queryOptions.getString(QueryOptions.INCLUDE) + " ");
        } else {
            sqlQueryString.append("SELECT * ");
        }
        sqlQueryString.append("FROM ").append(viewName).append(" ").append(StringUtils.join(explodes.toArray(), " ")).append(" ");
        sqlQueryString.append("WHERE ").append(StringUtils.join(filters, " AND "));

        System.out.println(sqlQueryString.toString());
        return sqlQueryString.toString();
    }

    private void processStudyQuery(String[] fields, Object value) {

        // sanity check, if there is any ...
        if (fields == null || fields.length == 0) {
            return;
        }

        String path = StringUtils.join(fields, ".", 1, fields.length - 1);
        String field = fields[fields.length - 1];

        switch (path) {
            case "studyId":
                filters.add("studies." + path + " = '" + value + "'");
                break;
            case "files":
                explodes.add("LATERAL VIEW explode(studies.files) act as file");

                switch (field) {
                    case "fileId":
                    case "call":
                        filters.add("file." + field + " = '" + value + "'");
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            case "format":
                filters.add("array_contains(studies.format, '" + value + "')");
                break;

            case "samplesData":
                explodes.add("LATERAL VIEW explode(studies.samplesData) act as sd");
                // investigate how to query GT
                break;

            default:
                break;
        }
    }

    private void processAnnotationQuery(String[] fields, Object value) {
        //            annotation.consequenceTypes.sequenceOntologyTerms.accession
        //            this.sql = "select * from vcf lateral view explode(annotation.consequenceTypes) act as ct
        //                                          lateral view explode(ct.sequenceOntologyTerms) ctso as so
        //                                          where so.accession = 'SO:0001566' and ct.geneName = 'BRCA2';

        // sanity check, if there is any ...
        if (fields == null || fields.length == 0) {
            return;
        }

        String path = StringUtils.join(fields, ".", 1, fields.length - 1);
        String field = fields[fields.length - 1];

        switch (path) {
            case "id":
            case "ancestralAllele":
            case "displayConsequenceType":
                filters.add("annotation." + path + " = '" + value + "'");
                break;
            case "xrefs":
                explodes.add("LATERAL VIEW explode(annotation.xrefs) act as xref");
                filters.add("xref." + field + " = '" + value + "'");
                break;
            case "hgvs":
                filters.add("array_contains(annotation.hgvs, '" + value + "')");
                break;

            // consequenceTypes is an array and therefore we have to use explode function
            case "consequenceTypes":
                explodes.add("LATERAL VIEW explode(annotation.consequenceTypes) act as ct");

                // we process most important fields inside consequenceTypes
                switch (field) {
                    case "geneName":
                    case "ensemblGeneId":
                    case "ensemblTranscriptId":
                    case "biotype":
                        filters.add("ct." + field + " = '" + value + "'");
                        break;
                    case "transcriptAnnotationFlags":
                        filters.add("array_contains(ct.transcriptAnnotationFlags, '" + value + "')");
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            // sequenceOntologyTerms is also an array and therefore we have to use explode function
            case "consequenceTypes.sequenceOntologyTerms":
                // we add both explode (order is kept) to the set (no repetitions allowed)
                explodes.add("LATERAL VIEW explode(annotation.consequenceTypes) act as ct");
                explodes.add("LATERAL VIEW explode(ct.sequenceOntologyTerms) ctso as so");

                switch (field) {
                    case "accession":
                    case "name":
                        filters.add("so." + field + " = '" + value + "'");
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            case "populationFrequencies":
                explodes.add("LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq");

                String[] studyAndPopulation = value.toString().split(":");
                filters.add("popfreq.study = " + studyAndPopulation[0] + " AND popfreq.population = " + studyAndPopulation[1]);

                break;

            case "conservation":
                String colName = "cons_" + field;
                switch (field) {
                    case "phylop":
                    case "phastCons":
                    case "gerp":
                        explodes.add("LATERAL VIEW explode(annotation.conservation) acons as " + colName);
                        filters.add(colName + ".source = '" + field + "' AND " + colName + ".score " + value);
                        break;
                    default:
                        break;
                }
                break;

            case "variantTraitAssociation":
                switch (field) {
                    case "clinvar":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.clinvar) avtac as clinvar");
                        filters.add("clinvar.accession = '" + value + "'");
                    case "cosmic":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.cosmic) avtac as cosmic");
                        filters.add("cosmic.mutationId = '" + value + "'");
                        break;
                    default:
                        break;
                }
                break;

            default:
                break;
        }
    }
}

