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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by imedina on 09/08/16.
 */
public class VariantParseQuery extends ParseQuery {

//    private static final Pattern POPULATION_PATTERN =
//            Pattern.compile("^([^=<>]+)::([^=<>]+)(<=|>=|!=|=~|==|<|>)([^=<>]+)$");
    private static final Pattern POPULATION_PATTERN =
            Pattern.compile("^([^=<>]+)::([^=<>]+)(<=?|>=?|!=|!?=?~|==?)([^=<>]+)$");
    private static final Pattern CONSERVATION_PATTERN =
            Pattern.compile("^([^=<>]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>]+.*)$");

    private static final String CONSERVATION_VIEW = "cons";
    private static final String SUBSTITUTION_VIEW = "subst";

    public VariantParseQuery() {
        super();
    }

    public String parse(Query query, QueryOptions queryOptions, String viewName) {

        Set<String> keySet = query.keySet();

        for (String key : keySet) {
            String value = (String) query.get(key);

            // sanity check
            if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
                throw new IllegalArgumentException("key or value are null or empty (key = "
                        + key + ", value = " + value + ")");
            }

            String[] fields = key.split("\\.");

            // First we check if there is any ...
            if (fields.length == 0) {
                System.out.println("length is 0, nothing to do");
                return null;
            }

            switch (fields[0]) {
                case "id":
                case "chromosome":
                case "type":
                    filters.add(processFilter(fields[0], value, true, false));
                    break;
                case "start":
                case "end":
                case "length":
                    filters.add(processFilter(fields[0], value, false, false));
                    break;
                case "region":
                    processRegionQuery(value, "chromosome", "start", "end");
                    break;
                case "names":
                    filters.add(processFilter("hgvs", value, true, true));
                    break;
                case "studies":
                    processStudyQuery(fields, value);
                    break;
                case "annotation":
                    processAnnotationQuery(fields, value);
                    break;
                default:
                    break;
            }
        }

        // Build the SQL string from the processed query using explodes and filters
        buildQueryString(viewName, queryOptions);
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
                filters.add(processFilter("studies." + path, (String) value, true, false));
                break;
            case "files":
                explodes.add("LATERAL VIEW explode(studies.files) act as file");

                switch (field) {
                    case "fileId":
                    case "call":
                        filters.add(processFilter("file." + field, (String) value, true, false));
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            case "format":
                filters.add(processFilter("studies.format", (String) value, true, true));
                break;

            case "samplesData":
                explodes.add("LATERAL VIEW explode(studies.samplesData) act as sd");
                // investigate how to query GT
                break;

            case "stats":
                auxAddPopulationFilters("study.studyId", "study_stats_key", "study_stats." + field,
                        (String) value, "population frequencies");
                explodes.add("LATERAL VIEW explode(studies) act as study");
                explodes.add("LATERAL VIEW explode(study.stats) act as study_stats_key, study_stats");
                break;

            default:
                break;
        }
    }

    private void processAnnotationQuery(String[] fields, String value) {
        // sanity check, if there is any ...
        if (fields == null || fields.length == 0) {
            return;
        }

        Matcher matcher;
        StringBuilder where;

        String field = fields[fields.length - 1];
        String path = StringUtils.join(fields, ".", 1, fields.length - 1);
        if (StringUtils.isEmpty(path)) {
            path = field;
        }

        switch (path) {
            case "id":
            case "ancestralAllele":
            case "displayConsequenceType":
                filters.add(processFilter("annotation." + path, value, true, false));
                break;
            case "xrefs":
                explodes.add("LATERAL VIEW explode(annotation.xrefs) act as xref");
                filters.add(processFilter("xref." + field, value, true, false));
                break;
            case "hgvs":
                // this is equivalent to case 'names' in parse function !!
                filters.add(processFilter("annotation.hgvs", value, true, true));
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
                        filters.add(processFilter("ct." + field, value, true, false));
                        break;
                    case "transcriptAnnotationFlags":
                        filters.add(processFilter("ct.transcriptAnnotationFlags", value, true, true));
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
                        filters.add(processFilter("so." + field, value, true, false));
                        break;
                    default:
                        if (value.startsWith("SO:")) {
                            filters.add(processFilter("so.accession", value, true, false));
                        } else {
                            filters.add(processFilter("so.name", value, true, false));
                        }
                        break;
                }
                break;

            case "consequenceTypes.proteinVariantAnnotation": {
                // we add both explode (order is kept) to the set (no repetitions allowed)
                explodes.add("LATERAL VIEW explode(annotation.consequenceTypes) act as ct");

                switch (field) {
                    case "uniprotAccession":
                    case "uniprotName":
                    case "position":
                    case "reference":
                    case "alternate":
                    case "uniprotVariantId":
                    case "functionalDescription":
                        filters.add(processFilter("ct.proteinVariantAnnotation." + field, value, true, false));
                        break;
                    case "substitutionScores": {
                        auxAddFilters(value, SUBSTITUTION_VIEW, "protein substitution scores");
                        // substitutionScores is also an array and therefore we have to use explode function
                        // we add both explode (order is kept) to the set (no repetitions allowed)
                        explodes.add("LATERAL VIEW explode(ct.proteinVariantAnnotation.substitutionScores) ctpvass as "
                                + SUBSTITUTION_VIEW);
                        break;
                    }
                    default: {
                        // error
                        System.err.format("Error: queries for '" + path + "." + field + "' not yet implemented!\n");
                        break;
                    }
                }
                break;
            }

            case "populationFrequencies": {
                auxAddPopulationFilters("popfreq.study", "popfreq.population", "popfreq." + field,
                        value, "population frequencies");
                explodes.add("LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq");
                break;
            }

            case "conservation": {
                auxAddFilters(value, CONSERVATION_VIEW, "conservation");
                explodes.add("LATERAL VIEW explode(annotation.conservation) acons as " + CONSERVATION_VIEW);
                break;
            }

            case "variantTraitAssociation":
                switch (field) {
                    case "clinvar":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.clinvar) avtac as clinvar");
                        filters.add(processFilter("clinvar.accession", value, true, false));
                    case "cosmic":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.cosmic) avtac as cosmic");
                        filters.add(processFilter("cosmic.mutationId", value, true, false));
                        break;
                    default:
                        break;
                }
                break;

            default:
                break;
        }
    }

    private void auxAddFilters(String value, String view, String label) {
        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("value is null or empty for " + label);
        }

        boolean or = value.contains(",");
        boolean and = value.contains(";");
        if (or && and) {
            throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
        }
        String logicalComparator = or ? " OR " : " AND ";

        String[] values = value.split("[,;]");

        Matcher matcher;
        StringBuilder where = new StringBuilder();

        if (values == null) {
            matcher = CONSERVATION_PATTERN.matcher(value);
            if (matcher.find()) {
                updateWhereString(view, matcher, where);
            } else {
                // error
                System.err.format("error: invalid expresion %s: abort!\n", value);
            }
        } else {
            matcher = CONSERVATION_PATTERN.matcher(values[0]);
            if (matcher.find()) {
                where.append("(");
                updateWhereString(view, matcher, where);
                for (int i = 1; i < values.length; i++) {
                    matcher = CONSERVATION_PATTERN.matcher(values[i]);
                    if (matcher.find()) {
                        where.append(logicalComparator);
                        updateWhereString(view, matcher, where);
                    } else {
                        // error
                        System.err.format("Error: invalid expresion %s: abort!\n", values[i]);
                    }
                }
                where.append(")");
            } else {
                // error
                System.err.format("Error: invalid expresion %s: abort!\n", values[0]);
            }
        }
        filters.add(where.toString());
    }


    private void auxAddPopulationFilters(String fieldName1, String fieldName2, String fieldName3,
                                         String value, String label) {
        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("value is null or empty for " + label);
        }

        boolean or = value.contains(",");
        boolean and = value.contains(";");
        if (or && and) {
            throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
        }
        String logicalComparator = or ? " OR " : " AND ";

        String[] values = value.split("[,;]");
        Matcher matcher;
        StringBuilder where = new StringBuilder();
        if (values == null) {
            matcher = POPULATION_PATTERN.matcher(value);

            if (matcher.find()) {
                updatePopWhereString(fieldName1, fieldName2, fieldName3, matcher, where);
            } else {
                // error
                System.err.format("Error: invalid expresion for %s: %s (pattern %s). Abort!\n",
                        label, value, matcher.pattern());
            }
        } else {
            matcher = POPULATION_PATTERN.matcher(values[0]);
            if (matcher.find()) {
                where.append("(");
                updatePopWhereString(fieldName1, fieldName2, fieldName3, matcher, where);
                for (int i = 1; i < values.length; i++) {
                    matcher = POPULATION_PATTERN.matcher(values[i]);
                    if (matcher.find()) {
                        where.append(logicalComparator);
                        updatePopWhereString(fieldName1, fieldName2, fieldName3, matcher, where);
                    } else {
                        // error
                        System.err.format("Error: invalid expresion for %s: %s (pattern %s). Abort!\n",
                                label, values[i], matcher.pattern());
                    }
                }
                where.append(")");
            } else {
                // error
                System.err.format("Error: invalid expresion for %s: %s (pattern %s). Abort!\n",
                        label, values[0], matcher.pattern());
            }
        }
        filters.add(where.toString());
    }

    private void updatePopWhereString(String fieldName1, String fieldName2, String fieldName3,
                                      Matcher matcher, StringBuilder where) {
        where.append("(")
                .append(fieldName1).append(" = '").append(matcher.group(1).trim())
                .append("' AND ")
                .append(fieldName2).append(" = '").append(matcher.group(2).trim())
                .append("' AND ")
                .append(fieldName3).append(matcher.group(3).trim())
                .append(matcher.group(4).trim()).append(")");
    }

//    private void updatePopWhereString(String field, String viewName, Matcher matcher, StringBuilder where) {
//        where.append("(").append(viewName).append(".study = '").append(matcher.group(1).trim())
//                .append("' AND ").append(viewName).append(".population = '").append(matcher.group(2).trim())
//                .append("' AND ").append(viewName).append(".").append(field).append(matcher.group(3).trim())
//                .append(matcher.group(4).trim()).append(")");
//    }

    private void updateWhereString(String view, Matcher matcher, StringBuilder where) {
        where.append("(").append(view).append(".source = '").append(matcher.group(1).trim())
                .append("' AND ").append(view).append(".score")
                .append(matcher.group(2).trim()).append(matcher.group(3).trim()).append(")");
    }
}

