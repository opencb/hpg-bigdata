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
public class VariantParseQuery {

    private static final Pattern REGION_PATTERN = Pattern.compile("[:-]");
    private static final Pattern POPULATION_PATTERN =
            Pattern.compile("^([^=<>:]+.*):([^=<>:]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>:]+.*)$");
    private static final Pattern CONSERVATION_PATTERN =
            Pattern.compile("^([^=<>]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>]+.*)$");

    private static final String CONSERVATION_VIEW = "cons";

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

            String value = (String) query.get(key);

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
                    processRegionQuery(value);
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
        if (queryOptions != null && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.INCLUDE))) {
            sqlQueryString.append("SELECT ").append(queryOptions.getString(QueryOptions.INCLUDE)).append(" ");
        } else {
            sqlQueryString.append("SELECT * ");
        }
        sqlQueryString.append("FROM ").append(viewName).append(" ").append(StringUtils.join(explodes.toArray(), " ")).append(" ");
        sqlQueryString.append("WHERE ").append(StringUtils.join(filters, " AND "));

        System.err.println(sqlQueryString.toString());

        return sqlQueryString.toString();
    }

    private void processRegionQuery(String value) {
        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("value is null or empty");
        }

        if (value.contains(";")) {
            throw new IllegalArgumentException("Semi-colon is not supported by region filter, only comma is allowed : "
                    + value);
        }

        String[] regions = value.split("[,]");
        StringBuilder filter = new StringBuilder();
        if (regions.length == 1) {
            filter.append(processRegion(regions[0].trim()));
        } else {
            filter.append("(");
            filter.append(processRegion(regions[0].trim()));
            for (int i = 1; i < regions.length; i++) {
                filter.append(" OR ");
                filter.append(processRegion(regions[i].trim()));
            }
            filter.append(")");
        }
        filters.add(filter.toString());
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
                        // error!!
                        break;
                }
                break;

            case "populationFrequencies": {
                explodes.add("LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq");

                if (StringUtils.isEmpty(value)) {
                    throw new IllegalArgumentException("value is null or empty for population frequencies");
                }

                boolean or = value.contains(",");
                boolean and = value.contains(";");
                if (or && and) {
                    throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
                }
                String logicalComparator = or ? " OR " : " AND ";

                String[] values = value.split("[,;]");

                where = new StringBuilder();
                if (values == null) {
                    matcher = POPULATION_PATTERN.matcher((String) value);
                    if (matcher.find()) {
                        updatePopWhereString(field, "popfreq", matcher, where);
                    } else {
                        // error
                        System.err.format("error: invalid expresion for population frequencies %s: abort!", value);
                    }
                } else {
                    matcher = POPULATION_PATTERN.matcher(values[0]);
                    if (matcher.find()) {
                        where.append("(");
                        updatePopWhereString(field, "popfreq", matcher, where);
                        for (int i = 1; i < values.length; i++) {
                            matcher = POPULATION_PATTERN.matcher(values[i]);
                            if (matcher.find()) {
                                where.append(logicalComparator);
                                updatePopWhereString(field, "popfreq", matcher, where);
                            } else {
                                // error
                                System.err.format("Error: invalid expresion %s: abort!", values[i]);
                            }
                        }
                        where.append(")");
                    } else {
                        // error
                        System.err.format("Error: invalid expresion %s: abort!", values[0]);
                    }
                }
                filters.add(where.toString());
                break;
            }

            case "conservation": {
                if (StringUtils.isEmpty(value)) {
                    throw new IllegalArgumentException("value is null or empty for conservation");
                }

                boolean or = value.contains(",");
                boolean and = value.contains(";");
                if (or && and) {
                    throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
                }
                String logicalComparator = or ? " OR " : " AND ";

                String[] values = value.split("[,;]");

                where = new StringBuilder();
                if (values == null) {
                    matcher = CONSERVATION_PATTERN.matcher(value);
                    if (matcher.find()) {
                        updateConsWhereString(matcher, where);
                    } else {
                        // error
                        System.err.format("error: invalid expresion %s: abort!", value);
                    }
                } else {
                    matcher = CONSERVATION_PATTERN.matcher(values[0]);
                    if (matcher.find()) {
                        where.append("(");
                        updateConsWhereString(matcher, where);
                        for (int i = 1; i < values.length; i++) {
                            matcher = CONSERVATION_PATTERN.matcher(values[i]);
                            if (matcher.find()) {
                                where.append(logicalComparator);
                                updateConsWhereString(matcher, where);
                            } else {
                                // error
                                System.err.format("Error: invalid expresion %s: abort!", values[i]);
                            }
                        }
                        where.append(")");
                    } else {
                        // error
                        System.err.format("Error: invalid expresion %s: abort!", values[0]);
                    }
                }
                filters.add(where.toString());
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

    private String processFilter(String key, String value, boolean isString, boolean isArray) {

        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("key or value are null or empty");
        }

        boolean or = value.contains(",");
        boolean and = value.contains(";");
        if (or && and) {
            throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
        }

        String op1 = isString ? " = '" : "";
        String op2 = isString ? "'" : "";
        String openArraContains = isArray ? "array_contains(" : "";
        String closeArrayContains = isArray ? ")" : "";

        String[] values = value.split("[,;]");
        StringBuilder filter = new StringBuilder();
        if (values.length == 1) {
            filter.append(openArraContains).append(key).append(op1).append(value).append(op2).append(closeArrayContains);
        } else {
            String logicalComparator = or ? " OR " : " AND ";
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

    private String processRegion(String region) {
        StringBuilder filter = new StringBuilder();
        filter.append("(");
        if (region.contains(":")) {
            String[] fields = REGION_PATTERN.split(region, -1);
            if (fields.length == 3) {
                filter.append("chromosome = '").append(fields[0]).append("'");
                filter.append(" AND start >= ").append(fields[1]);
                filter.append(" AND end <= ").append(fields[2]);
            } else {
                if (fields.length == 2) {
                    filter.append("chromosome = '").append(fields[0]).append("'");
                    filter.append(" AND start >= ").append(fields[1]);
                }
            }
        } else {
            filter.append("chromosome = '").append(region).append("'");
        }
        filter.append(")");
        return filter.toString();
    }

    private void updatePopWhereString(String field, String viewName, Matcher matcher, StringBuilder where) {
        where.append("(").append(viewName).append(".study = '").append(matcher.group(1).trim())
                .append("' AND ").append(viewName).append(".population = '").append(matcher.group(2).trim())
                .append("' AND ").append(viewName).append(".").append(field).append(matcher.group(3).trim())
                .append(matcher.group(4).trim()).append(")");
    }

    private void updateConsWhereString(Matcher matcher, StringBuilder where) {
        where.append("(").append(CONSERVATION_VIEW).append(".source = '").append(matcher.group(1).trim())
                .append("' AND ").append(CONSERVATION_VIEW).append(".score")
                .append(matcher.group(2).trim()).append(matcher.group(3).trim()).append(")");
    }

    public Set<String> getExplodes() {
        return explodes;
    }

    public void setExplodes(Set<String> explodes) {
        this.explodes = explodes;
    }

}

