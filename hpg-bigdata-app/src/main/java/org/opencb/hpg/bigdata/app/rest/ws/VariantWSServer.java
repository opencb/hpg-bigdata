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

package org.opencb.hpg.bigdata.app.rest.ws;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.opencb.hpg.bigdata.app.cli.CliUtils;
import org.opencb.hpg.bigdata.app.cli.options.VariantCommandOptions;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{version}/variant")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cohorts", position = 9, description = "Methods for working with 'cohorts' endpoint")
public class VariantWSServer {


    public VariantWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
//        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/sql")
    @ApiOperation(value = "Create a cohort", position = 1, notes = "", response = String.class)
    public Response getSqlQuery(@ApiParam(value = "table", required = true) @QueryParam("table") String table,
                                @ApiParam(value = "id") @QueryParam("id") String id,
                                @ApiParam(value = "ancestralAllele") @QueryParam("ancestralAllele") String ancestralAllele,
                                @ApiParam(value = "displayConsequenceType")
                                @QueryParam("displayConsequenceType") String displayConsequenceType,
                                @ApiParam(value = "xrefs") @QueryParam("xrefs") String xrefs,
                                @ApiParam(value = "hgvs") @QueryParam("hgvs") String hgvs,
                                @ApiParam(value = "consequenceTypes") @QueryParam("consequenceTypes") String consequenceTypes,
                                @ApiParam(value = "consequenceTypes.sequenceOntologyTerms.accession")
                                @QueryParam("consequenceTypes.sequenceOntologyTerms.accession") String consequenceSoAccession,
                                @ApiParam(value = "consequenceTypes.sequenceOntologyTerms.name")
                                @QueryParam("consequenceTypes.sequenceOntologyTerms.name") String consequenceSoName,
                                @ApiParam(value = "populationFrequencies")
                                @QueryParam("populationFrequencies") String populationFrequencies,
                                @ApiParam(value = "conservation") @QueryParam("conservation") String conservation,
                                @ApiParam(value = "variantTraitAssociation")
                                @QueryParam("variantTraitAssociation") String variantTraitAssociation) {
        try {
            SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.load("");
            vd.createOrReplaceTempView(table);

            // create cli options
            VariantCommandOptions variantCommandOptions;
            variantCommandOptions = CliUtils.createVariantCommandOptions(id, ancestralAllele, displayConsequenceType,
                    xrefs, hgvs, consequenceTypes, consequenceSoAccession, consequenceSoName, populationFrequencies,
                    conservation, variantTraitAssociation);

            // add filters
            CliUtils.addVariantFilters(variantCommandOptions, vd);
            vd.update();

            // get the sql and return
            String sql = vd.getSql();
            return Response.ok(sql).build();
        } catch (Exception e) {
            return Response.ok(e.toString()).build();
        }
    }

    @GET
    @Path("/query")
    public Response query(@ApiParam(value = "table", required = true) @QueryParam("table") String table,
                          @ApiParam(value = "id") @QueryParam("id") String id,
                          @ApiParam(value = "ancestralAllele") @QueryParam("ancestralAllele") String ancestralAllele,
                          @ApiParam(value = "displayConsequenceType")
                          @QueryParam("displayConsequenceType") String displayConsequenceType,
                          @ApiParam(value = "xrefs") @QueryParam("xrefs") String xrefs,
                          @ApiParam(value = "hgvs") @QueryParam("hgvs") String hgvs,
                          @ApiParam(value = "consequenceTypes") @QueryParam("consequenceTypes") String consequenceTypes,
                          @ApiParam(value = "consequenceTypes.sequenceOntologyTerms.accession")
                          @QueryParam("consequenceTypes.sequenceOntologyTerms.accession") String consequenceSoAccession,
                          @ApiParam(value = "consequenceTypes.sequenceOntologyTerms.name")
                          @QueryParam("consequenceTypes.sequenceOntologyTerms.name") String consequenceSoName,
                          @ApiParam(value = "populationFrequencies")
                          @QueryParam("populationFrequencies") String populationFrequencies,
                          @ApiParam(value = "conservation") @QueryParam("conservation") String conservation,
                          @ApiParam(value = "variantTraitAssociation")
                          @QueryParam("variantTraitAssociation") String variantTraitAssociation) {

        try {
            SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.createOrReplaceTempView(table);
            vd.load("");

            // create cli options
            VariantCommandOptions variantCommandOptions;
            variantCommandOptions = CliUtils.createVariantCommandOptions(id, ancestralAllele, displayConsequenceType,
                    xrefs, hgvs, consequenceTypes, consequenceSoAccession, consequenceSoName, populationFrequencies,
                    conservation, variantTraitAssociation);

            // add filters
            CliUtils.addVariantFilters(variantCommandOptions, vd);
            vd.update();

            // convert to JSON and return
            Dataset<String> stringDataset = vd.toJSON();
            return Response.ok(stringDataset.toString()).build();
        } catch (Exception e) {
            return Response.ok(e.toString()).build();
        }


    }
}
