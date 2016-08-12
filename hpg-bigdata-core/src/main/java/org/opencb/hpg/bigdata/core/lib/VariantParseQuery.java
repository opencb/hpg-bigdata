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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by imedina on 09/08/16.
 */
public class VariantParseQuery {

    private Set<String> explodes;
    private List<String> filters;

    private StringBuilder sqlQueryString;

    @Deprecated
    private String op1, op2, binaryOp;
    @Deprecated
    private String[] values;

    public VariantParseQuery() {
        explodes = new LinkedHashSet<>();
        filters = new ArrayList<>();

        sqlQueryString = new StringBuilder();
    }

    @Deprecated
    private void preProcessing(String value, boolean isString) {
        values = null;
        binaryOp = "";
        if (value.contains(",")) {
            values = value.split(",");
            binaryOp = " OR ";
        } else if (value.contains(";")) {
            values = value.split(";");
            binaryOp = " AND ";
        }

        if (isString) {
            op1 = " = '";
            op2 = "'";
        } else {
            op1 = "";
            op2 = "";
        }
    }

    @Deprecated
    private void processValue(String key, String value, boolean isString) {
        preProcessing(value, isString);

        StringBuilder where = new StringBuilder();
        if (values == null) {
            where.append(key).append(op1).append(value).append(op2);
        } else {
            where.append("(").append(key).append(op1).append(values[0]).append(op2);
            for (int i=1; i < values.length; i++) {
                where.append(binaryOp);
                where.append(key).append(op1).append(values[i]).append(op2);
            }
            where.append(")");
        }
        filters.add(where.toString());
    }

    @Deprecated
    private void processArrayContains(String key, String value, boolean isString) {
        preProcessing(value, isString);

        // values on the list are considered as String
        StringBuilder where = new StringBuilder();
        if (values == null) {
            where.append("array_contains(").append(key).append(op1).append(value).append(op2).append(")");
        } else {
            where.append("(array_contains(").append(key).append(op1).append(values[0]).append(op2).append(")");
            for (int i = 1; i < values.length; i++) {
                where.append(binaryOp);
                where.append("array_contains(").append(key).append(op1).append(values[i]).append(op2).append(")");
            }
            where.append(")");
        }
        filters.add(where.toString());
    }


    public String parse(Query query, QueryOptions queryOptions, String viewName) {

        Set<String> keySet = query.keySet();
        for (String key : keySet) {
            String[] fields = key.split("\\.");

            // First we check if there is any ...
            if (fields.length == 0) {
                return null;
            }

            String value = (String) query.get(key);

            switch (fields[0]) {
                case "id":
                case "chromosome":
                case "type":
                    processFilter(fields[0], value, true, false);
                    break;
                case "start":
                case "end":
                case "length":
                    processFilter(fields[0], value, false, false);
                    break;
                case "region":
                    break;
                case "names":
                    processFilter("hgvs", value, true, true);
                    //filters.add("array_contains(annotation.hgvs, '" + fields[fields.length - 1] + "')");
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
            sqlQueryString.append("SELECT ").append(queryOptions.getString(QueryOptions.INCLUDE)).append(" ");
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
                processFilter("studies." + path, (String) value, true, false);
                break;
            case "files":
                explodes.add("LATERAL VIEW explode(studies.files) act as file");

                switch (field) {
                    case "fileId":
                    case "call":
                        processFilter("file." + field, (String) value, true, false);
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            case "format":
                processFilter("studies.format", (String) value, true, true);
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
        // sanity check, if there is any ...
        if (fields == null || fields.length == 0) {
            return;
        }

        Matcher matcher;
        Pattern pattern;
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
                processFilter("annotation." + path, (String) value, true, false);
                break;
            case "xrefs":
                explodes.add("LATERAL VIEW explode(annotation.xrefs) act as xref");
                processFilter("xref." + field, (String) value, true, false);
                break;
            case "hgvs":
                // this is equivalent to case 'names' in parse function !!
                processFilter("annotation.hgvs", (String) value, true, true);
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
                        processFilter("ct." + field, (String) value, true, false);
                        break;
                    case "transcriptAnnotationFlags":
                        processFilter("ct.transcriptAnnotationFlags", (String) value, true, true);
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
                        processFilter("so." + field, (String) value, true, false);
                        break;
                    default:
                        // error!!
                        break;
                }
                break;

            case "populationFrequencies":
                explodes.add("LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq");
                preProcessing((String) value, false);

                pattern = Pattern.compile("^([^=<>:]+.*):([^=<>:]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>:]+.*)$");

                where = new StringBuilder();
                if (values == null) {
                    matcher = pattern.matcher((String) value);
                    if (matcher.find()) {
                        updatePopWhereString(field, "popfreq", matcher, where);
                    } else {
                        // error
                        System.out.println("error: invalid expresion " + value + ": abort!");
                    }
                } else {
                    matcher = pattern.matcher(values[0]);
                    if (matcher.find()) {
                        where.append("(");
                        updatePopWhereString(field, "popfreq", matcher, where);
                        for (int i=1; i < values.length; i++) {
                            matcher = pattern.matcher(values[i]);
                            if (matcher.find()) {
                                where.append(binaryOp);
                                updatePopWhereString(field, "popfreq", matcher, where);
                            } else {
                                // error
                                System.out.println("error: invalid expresion " + values[i] + ": abort!");
                            }
                        }
                        where.append(")");
                    } else {
                        // error
                        System.out.println("error: invalid expresion " + values[0] + ": abort!");
                    }
                }
                filters.add(where.toString());
                break;

            case "conservation":
                // is it expensive computationally? Maybe we have to use a map to check if
                // a given cons has already explode before exploding!
                explodes.add("LATERAL VIEW explode(annotation.conservation) acons as cons_phylop");
                explodes.add("LATERAL VIEW explode(annotation.conservation) acons as cons_phastCons");
//                explodes.add("LATERAL VIEW explode(annotation.conservation) acons as cons_gerp");
                preProcessing((String) value, false);

                pattern = Pattern.compile("^([^=<>]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>]+.*)$");

                where = new StringBuilder();
                if (values == null) {
                    matcher = pattern.matcher((String) value);
                    if (matcher.find()) {
                        updateConsWhereString("cons_" + matcher.group(1).trim(), matcher, where);
                    } else {
                        // error
                        System.out.println("error: invalid expresion " + value + ": abort!");
                    }
                } else {
                    matcher = pattern.matcher(values[0]);
                    if (matcher.find()) {
                        where.append("(");
                        updateConsWhereString("cons_" + matcher.group(1).trim(), matcher, where);
                        for (int i=1; i < values.length; i++) {
                            matcher = pattern.matcher(values[i]);
                            if (matcher.find()) {
                                where.append(binaryOp);
                                updateConsWhereString("cons_" + matcher.group(1).trim(), matcher, where);
                            } else {
                                // error
                                System.out.println("error: invalid expresion " + values[i] + ": abort!");
                            }
                        }
                        where.append(")");
                    } else {
                        // error
                        System.out.println("error: invalid expresion " + values[0] + ": abort!");
                    }
                }
                filters.add(where.toString());
                break;

            case "variantTraitAssociation":
                switch (field) {
                    case "clinvar":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.clinvar) avtac as clinvar");
                        processFilter("clinvar.accession", (String) value, true, false);
                    case "cosmic":
                        explodes.add("LATERAL VIEW explode(annotation.variantTraitAssociation.cosmic) avtac as cosmic");
                        processFilter("cosmic.mutationId", (String) value, true, false);
                        break;
                    default:
                        break;
                }
                break;

            default:
                break;
        }
    }

    private String processFilter(String key, String value, boolean isString, boolean isArray) {

        if (StringUtils.isEmpty(key)|| StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("key or value are null or empty");
        }

        boolean or = value.contains(",");
        boolean and = value.contains(";");
        if (or && and) {
            throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
        }

        String op1 = (isString) ? " = '" : "";
        String op2 = (isString) ? "'" : "";
        String openArraContains = (isArray) ? "array_contains(" : "";
        String closeArrayContains = (isArray) ? ")" : "";

        String[] values = value.split("[,;]");
        StringBuilder filter = new StringBuilder();
        if (values.length == 1) {
            filter.append(openArraContains).append(key).append(op1).append(value).append(op2).append(closeArrayContains);
        } else {
            String logicalComparator = (or) ? " OR " : " AND ";
            filter.append("(");
            filter.append(openArraContains).append(key).append(op1).append(values[0]).append(op2).append(closeArrayContains);
            for (int i = 1; i < values.length; i++) {
                filter.append(logicalComparator);
                filter.append(openArraContains).append(key).append(op1).append(values[i]).append(op2).append(closeArrayContains);
            }
            filter.append(")");
        }

        return filter.toString();
    }

    private void updatePopWhereString(String field, String viewName, Matcher matcher, StringBuilder where) {
        where.append("(").append(viewName).append(".study = '").append(matcher.group(1).trim())
                .append("' AND ").append(viewName).append(".population = '").append(matcher.group(2).trim())
                .append("' AND ").append(viewName).append(".").append(field).append(matcher.group(3).trim())
                .append(matcher.group(4).trim()).append(")");
    }

    private void updateConsWhereString(String viewName, Matcher matcher, StringBuilder where) {
        where.append("(").append(viewName).append(".source = '").append(matcher.group(1).trim())
                .append("' AND ").append(viewName).append(".score")
                .append(matcher.group(2).trim()).append(matcher.group(3).trim()).append(")");
    }
}

