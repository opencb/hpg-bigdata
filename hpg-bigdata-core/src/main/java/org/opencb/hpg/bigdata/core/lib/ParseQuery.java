package org.opencb.hpg.bigdata.core.lib;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by joaquin on 8/19/16.
 */
public abstract  class ParseQuery {

    protected static final Pattern REGION_PATTERN = Pattern.compile("[:-]");
    protected static final Pattern COMPARISON_PATTERN =
            Pattern.compile("^(<=?|>=?|!=|!?=?~|==?)([^=<>]+.*)$");

    protected Set<String> explodes;
    protected List<String> filters;

    protected StringBuilder sqlQueryString;

    public ParseQuery() {
        explodes = new LinkedHashSet<>();
        filters = new ArrayList<>();

        sqlQueryString = new StringBuilder();
    }

    public abstract String parse(Query query, QueryOptions queryOptions, String viewName);

    protected void buildSimpleQueryString(String viewName, QueryOptions queryOptions) {

//        if (queryOptions != null && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.INCLUDE))) {
//            sqlQueryString.append("SELECT ").append(queryOptions.getString(QueryOptions.INCLUDE)).append(" ");
//        } else {
//            sqlQueryString.append("SELECT * ");
//        }
        sqlQueryString.append("SELECT * ");
        sqlQueryString.append("FROM ").append(viewName).append(" ").append(StringUtils.join(explodes.toArray(), " ")).append(" ");
        if (filters.size() > 0) {
            sqlQueryString.append("WHERE ").append(StringUtils.join(filters, " AND "));
        }
    }

    protected String processFilter(String key, String value, boolean isString, boolean isArray) {

        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("key or value are null or empty (key = "
                    + key + ", value = " + value + ")");
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

    protected void processRegionQuery(String value, String chromLabel,
                                      String startLabel, String endLabel) {
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
            filter.append(processRegion(regions[0].trim(), chromLabel, startLabel, endLabel));
        } else {
            filter.append("(");
            filter.append(processRegion(regions[0].trim(), chromLabel, startLabel, endLabel));
            for (int i = 1; i < regions.length; i++) {
                filter.append(" OR ");
                filter.append(processRegion(regions[i].trim(), chromLabel, startLabel, endLabel));
            }
            filter.append(")");
        }
        filters.add(filter.toString());
    }

    private String processRegion(String region, String chromLabel,
                                 String startLabel, String endLabel) {
        StringBuilder filter = new StringBuilder();
        filter.append("(");
        if (region.contains(":")) {
            String[] fields = REGION_PATTERN.split(region, -1);
            if (fields.length == 3) {
                filter.append(chromLabel).append(" = '").append(fields[0]).append("'");
                filter.append(" AND ").append(startLabel).append(" >= ").append(fields[1]);
                filter.append(" AND ").append(endLabel).append(" <= ").append(fields[2]);
            } else {
                if (fields.length == 2) {
                    filter.append(chromLabel).append(" = '").append(fields[0]).append("'");
                    filter.append(" AND ").append(startLabel).append(" >= ").append(fields[1]);
                }
            }
        } else {
            filter.append(chromLabel).append(" = '").append(region).append("'");
        }
        filter.append(")");
        return filter.toString();
    }

    public Set<String> getExplodes() {
        return explodes;
    }

    public void setExplodes(Set<String> explodes) {
        this.explodes = explodes;
    }
}
