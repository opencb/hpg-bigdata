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

import org.opencb.commons.datastore.core.Query;

/**
 * Created by imedina on 04/08/16.
 */
public class VariantDataset extends ParentDataset<VariantDataset> {

    private VariantParseQuery variantParseQuery;
    private String sql;

    public VariantDataset() {
        super();
        query = new Query();
        variantParseQuery = new VariantParseQuery();
    }

    @Override
    protected void updateDataset(Query query) {
        if (this.sql == null) {
            this.sql = variantParseQuery.parse(query, null);
//            this.sql = "select * from " + this.viewName + " lateral view explode(annotation.consequenceTypes) act as ct lateral view"
//                        + " explode(ct.sequenceOntologyTerms) ctso as so where so.accession = 'SO:0001566'";
            this.ds = this.sqlContext.sql(this.sql);
        }
    }

    public VariantDataset annotationfilter(String key, String value) {
        query.put("annotation." + key, value);
        return this;
    }


    public void showMe() {
        System.out.println("------> VariatDataset");
    }
}
