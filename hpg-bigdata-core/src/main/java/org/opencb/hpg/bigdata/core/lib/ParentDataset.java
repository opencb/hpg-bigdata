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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.opencb.commons.datastore.core.Query;
import scala.Symbol;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;


/**
 * Created by imedina on 04/08/16.
 */
public abstract class ParentDataset<T> {

    protected Query query;
    protected String viewName;
    protected Dataset<Row> ds;
    protected SQLContext sqlContext;
    protected SparkSession sparkSession;

    public ParentDataset() {
        ds = null;
        sqlContext = null;

        query = new Query();
    }

    public void load(String filename, SparkSession sparkSession) throws Exception {
        this.sparkSession = sparkSession;
        sqlContext = new SQLContext(sparkSession);

        if (StringUtils.endsWithAny(filename, "avro", "avro.gz", "avro.sz")) {
            ds = sqlContext.read().format("com.databricks.spark.avro").load(filename);
        } else if (StringUtils.endsWithAny(filename, "json", "json.gz")) {
            ds = sqlContext.read().json(filename);
        } else {
            ds = sqlContext.read().load(filename);
        }
    }

    protected abstract void updateDataset(Query query);

    public void update() {
        updateDataset(query);
    }


    public ParentDataset<T> agg(Column expr, Column... exprs) {
        ds = ds.agg(expr, exprs);
        return this;
    }

    public ParentDataset<T> agg(Column expr, Seq<Column> exprs) {
        ds = ds.agg(expr, exprs);
        return this;
    }

    public ParentDataset<T> agg(scala.collection.immutable.Map<String, String> exprs) {
        ds = ds.agg(exprs);
        return this;
    }

    public ParentDataset<T> agg(Map<String, String> exprs) {
        ds = ds.agg(exprs);
        return this;
    }

    public ParentDataset<T> agg(Tuple2<String, String> aggExpr, Seq<Tuple2<String, String>> aggExprs) {
        ds = ds.agg(aggExpr, aggExprs);
        return this;
    }

    public ParentDataset<T> alias(String alias)  {
        ds = ds.alias(alias);
        return this;
    }

    public ParentDataset<T> alias(Symbol alias) {
        ds = ds.alias(alias);
        return this;
    }

    public Column apply(String colName) {
        return ds.apply(colName);
    }

    public ParentDataset<T> as(String alias) {
        ds = ds.as(alias);
        return this;
    }

    public ParentDataset<T> as(scala.Symbol alias) {
        ds = ds.as(alias);
        return this;
    }

    public ParentDataset<T> cache() {
        ds = ds.cache();
        return this;
    }

    public ParentDataset<T> coalesce(int numPartitions) {
        ds = ds.coalesce(numPartitions);
        return this;
    }

    public Column col(String colName) {
        return ds.col(colName);
    }

    public Object collect() {
        return ds.collect();
    }

    public List<Row> collectAsList() {
        return ds.collectAsList();
    }

    protected int collectToPython() {
        return ds.collectToPython();
    }

    public String[] columns() {
        return ds.columns();
    }

    long count() {
        updateDataset(query);
        return ds.count();
    }

    public RelationalGroupedDataset cube(Column... cols) {
        return ds.cube(cols);
    }

    public RelationalGroupedDataset cube(Seq<Column> cols) {
        return ds.cube(cols);
    }

    public RelationalGroupedDataset cube(String col1, Seq<String> cols) {
        return ds.cube(col1, cols);
    }

    public RelationalGroupedDataset cube(String col1, String... cols) {
        return ds.cube(col1, cols);
    }

    public ParentDataset<T> describe(Seq<String> cols) {
        ds = ds.describe(cols);
        return this;
    }

    public ParentDataset<T> describe(String... cols) {
        ds = ds.describe(cols);
        return this;
    }

    public ParentDataset<T> distinct() {
        ds = ds.distinct();
        return this;
    }

    public ParentDataset<T> drop(Column col) {
        ds = ds.drop(col);
        return this;
    }

    public ParentDataset<T> drop(String colName) {
        ds = ds.drop(colName);
        return this;
    }

    public ParentDataset<T> dropDuplicates() {
        ds = ds.dropDuplicates();
        return this;
    }

    public ParentDataset<T> dropDuplicates(Seq<String> colNames) {
        ds = ds.dropDuplicates(colNames);
        return this;
    }

    public ParentDataset<T> dropDuplicates(String[] colNames) {
        ds = ds.dropDuplicates(colNames);
        return this;
    }

    public Tuple2<String, String>[] dtypes() {
        return ds.dtypes();
    }

    public ParentDataset<T> except(Dataset<Row> other) {
        ds = ds.except(other);
        return this;
    }

    public void explain() {
        ds.explain();
    }

    public void explain(boolean extended) {
        ds.explain(extended);
    }

//    public ParentDataset<T> explode(Seq<Column> input, scala.Function1<Row,TraversableOnce<A>> f,
//                                    scala.reflect.api.TypeTags.TypeTag<A> evidence) {
//        ds = ds.explode(input, evidence);
//        return this;
//    }
//    public <A,B> DataFrame explode(String inputColumn, String outputColumn, scala.Function1<A,TraversableOnce<B>> f,
//                            scala.reflect.api.TypeTags.TypeTag<B> evidence)


    public ParentDataset<T> filter(Column condition) {
        ds = ds.filter(condition);
        return this;
    }

    public ParentDataset<T> filter(String conditionExpr) {
        updateDataset(query);
        ds = ds.filter(conditionExpr);
        return this;
    }

    public Row first() {
        return ds.first();
    }

    public void foreach(scala.Function1<Row, scala.runtime.BoxedUnit> f) {
        ds.foreach(f);
    }

    public void foreachPartition(scala.Function1<scala.collection.Iterator<Row>, scala.runtime.BoxedUnit> f) {
        ds.foreachPartition(f);
    }

    public RelationalGroupedDataset groupBy(Column... cols) {
        return ds.groupBy(cols);
    }

    public RelationalGroupedDataset groupBy(Seq<Column> cols) {
        return ds.groupBy(cols);
    }

    public RelationalGroupedDataset groupBy(String col1, Seq<String> cols) {
        return ds.groupBy(col1, cols);
    }

    public RelationalGroupedDataset groupBy(String col1, String... cols) {
        return ds.groupBy(col1, cols);
    }

    public Row head() {
        return ds.head();
    }

    public Object head(int n) {
        return ds.head(n);
    }

    public String[] inputFiles() {
        return ds.inputFiles();
    }

    public ParentDataset<T> intersect(Dataset<Row> other) {
        ds = ds.intersect(other);
        return this;
    }

    boolean isLocal() {
        return ds.isLocal();
    }

    public JavaRDD<Row> javaRDD() {
        return ds.javaRDD();
    }

    protected JavaRDD<byte[]> javaToPython() {
        return ds.javaToPython();
    }

    public ParentDataset<T> join(Dataset<Row> right) {
        ds = ds.join(right);
        return this;
    }

    public ParentDataset<T> join(Dataset<Row> right, Column joinExprs) {
        ds = ds.join(right, joinExprs);
        return this;
    }

    public ParentDataset<T> join(Dataset<Row> right, Column joinExprs, String joinType) {
        ds = ds.join(right, joinExprs, joinType);
        return this;
    }

    public ParentDataset<T> join(Dataset<Row> right, Seq<String> usingColumns) {
        ds = ds.join(right, usingColumns);
        return this;
    }

    public ParentDataset<T> join(Dataset<Row> right, Seq<String> usingColumns, String joinType) {
        ds = ds.join(right, usingColumns, joinType);
        return this;
    }

    public ParentDataset<T> join(Dataset<Row> right, String usingColumn) {
        ds = ds.join(right, usingColumn);
        return this;
    }

    public ParentDataset<T> limit(int n) {
        ds = ds.limit(n);
        return this;
    }

    protected org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan() {
        return ds.logicalPlan();
    }

    /*
    public Dataset<U> map(Function1<Row, U> f, Encoder<U> encoder) {
        return ds.map(f, encoder);
    }

    public Dataset<U> map(MapFunction<Row, U> f, Encoder<U> encoder) {
        return ds.map(f, encoder);
    }

    public <R> RDD<R> mapPartitions(scala.Function1<scala.collection.Iterator<Row>, scala.collection.Iterator<R>> f,
                                    scala.reflect.ClassTag<R> evidence) {
        return ds.mapPartitions(f, evidence);
    }
*/

    public DataFrameNaFunctions na() {
        return ds.na();
    }

    protected Seq<org.apache.spark.sql.catalyst.expressions.Expression> numericColumns() {
        return ds.numericColumns();
    }

    public ParentDataset<T> orderBy(Column... sortExprs) {
        ds = ds.orderBy(sortExprs);
        return this;
    }

    public ParentDataset<T> orderBy(Seq<Column> sortExprs) {
        ds = ds.orderBy(sortExprs);
        return this;
    }

    public ParentDataset<T> orderBy(String sortCol, Seq<String> sortCols) {
        ds = ds.orderBy(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> orderBy(String sortCol, String... sortCols) {
        ds = ds.orderBy(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> persist() {
        ds = ds.persist();
        return this;
    }

    public ParentDataset<T> persist(StorageLevel newLevel) {
        ds = ds.persist(newLevel);
        return this;
    }

    public void printSchema() {
        ds.printSchema();
    }

    /*
    public T[] randomSplit(double[] weighscala.reflect.ClassTagts) {
        ds = ds.randomSplit();
        return this;
    }
    public T[] randomSplit(double[] weights, long seed)
    */

    public RDD<Row> rdd() {
        return ds.rdd();
    }

    @Deprecated
    public void registerTempTable(String tableName) {
        this.viewName = tableName;
        ds.registerTempTable(tableName);
    }

    public void createOrReplaceTempView(String viewName) {
        this.viewName = viewName;
        ds.createOrReplaceTempView(viewName);
    }

    public void createTempView(String viewName) throws AnalysisException {
        this.viewName = viewName;
        ds.createTempView(viewName);
    }

    public ParentDataset<T> repartition(Column... partitionExprs) {
        ds = ds.repartition(partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions) {
        ds = ds.repartition(numPartitions);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions, Column... partitionExprs) {
        ds = ds.repartition(numPartitions, partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions, Seq<Column> partitionExprs) {
        ds = ds.repartition(numPartitions, partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(Seq<Column> partitionExprs) {
        ds = ds.repartition(partitionExprs);
        return this;
    }

    protected org.apache.spark.sql.catalyst.expressions.NamedExpression resolve(String colName) {
        return ds.resolve(colName);
    }

    public RelationalGroupedDataset rollup(Column... cols) {
        return ds.rollup(cols);
    }

    public RelationalGroupedDataset rollup(Seq<Column> cols) {
        return ds.rollup(cols);
    }

    public RelationalGroupedDataset rollup(String col1, Seq<String> cols) {
        return ds.rollup(col1, cols);
    }

    public RelationalGroupedDataset rollup(String col1, String... cols) {
        return ds.rollup(col1, cols);
    }

    public ParentDataset<T> sample(boolean withReplacement, double fraction) {
        ds = ds.sample(withReplacement, fraction);
        return this;
    }

    public ParentDataset<T> sample(boolean withReplacement, double fraction, long seed) {
        ds = ds.sample(withReplacement, fraction, seed);
        return this;
    }

    public StructType schema() {
        return ds.schema();
    }

    public ParentDataset<T> select(Column... cols) {
        ds = ds.select(cols);
        return this;
    }

    public ParentDataset<T> select(Seq<Column> cols) {
        ds = ds.select(cols);
        return this;
    }

    public ParentDataset<T> select(String col, Seq<String> cols) {
        ds = ds.select(col, cols);
        return this;
    }

    public ParentDataset<T> select(String col, String... cols) {
        ds = ds.select(col, cols);
        return this;
    }

    public ParentDataset<T> selectExpr(Seq<String> exprs) {
        ds = ds.selectExpr(exprs);
        return this;
    }

    public ParentDataset<T> selectExpr(String... exprs) {
        ds = ds.selectExpr(exprs);
        return this;
    }

    public void show() {
        this.show(20);
    }

    public void show(boolean truncate) {
        ds.show(truncate);
    }

    public void show(int numRows) {
        updateDataset(query);
        ds.show(numRows);
    }

    public void show(int numRows, boolean truncate) {
        ds.show(numRows, truncate);
    }

    public ParentDataset<T> sort(Column... sortExprs) {
        ds = ds.sort(sortExprs);
        return this;
    }

    public ParentDataset<T> sort(Seq<Column> sortExprs) {
        ds = ds.sort(sortExprs);
        return this;
    }

    public ParentDataset<T> sort(String sortCol, Seq<String> sortCols) {
        ds = ds.sort(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sort(String sortCol, String... sortCols) {
        ds = ds.sort(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(Column... sortExprs) {
        ds = ds.sortWithinPartitions(sortExprs);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(Seq<Column> sortExprs) {
        ds = ds.sortWithinPartitions(sortExprs);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(String sortCol, Seq<String> sortCols) {
        ds = ds.sortWithinPartitions(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(String sortCol, String... sortCols) {
        ds = ds.sortWithinPartitions(sortCol, sortCols);
        return this;
    }

    public SQLContext sqlContext() {
        return ds.sqlContext();
    }

    public DataFrameStatFunctions stat() {
        return ds.stat();
    }

    public Object take(int n) {
        return ds.take(n);
    }

    public List<Row> takeAsList(int n) {
        return ds.takeAsList(n);
    }

    public ParentDataset<T> toDF() {
        ds = ds.toDF();
        return this;
    }

    public ParentDataset<T> toDF(Seq<String> colNames) {
        ds = ds.toDF(colNames);
        return this;
    }

    public ParentDataset<T> toDF(String... colNames) {
        ds = ds.toDF(colNames);
        return this;
    }

    public JavaRDD<Row> toJavaRDD() {
        return ds.toJavaRDD();
    }

    public Dataset<String> toJSON() {
        return ds.toJSON();
    }

//    <U> DataFrame transform(scala.Function1<DataFrame,DataFrame> t)

    public ParentDataset<T> union(Dataset<Row> other) {
        ds = ds.union(other);
        return this;
    }

    @Deprecated
    public ParentDataset<T> unionAll(Dataset<Row> other) {
        ds = ds.unionAll(other);
        return this;
    }

    public ParentDataset<T> unpersist() {
        ds = ds.unpersist();
        return this;
    }

    public ParentDataset<T> unpersist(boolean blocking) {
        ds = ds.unpersist(blocking);
        return this;
    }

    public ParentDataset<T> where(Column condition) {
        ds = ds.where(condition);
        return this;
    }

    public ParentDataset<T> where(String conditionExpr) {
        ds = ds.where(conditionExpr);
        return this;
    }

    public ParentDataset<T> withColumn(String colName, Column col) {
        ds = ds.withColumn(colName, col);
        return this;
    }

    public ParentDataset<T> withColumnRenamed(String existingName, String newName) {
        ds = ds.withColumnRenamed(existingName, newName);
        return this;
    }

    public DataFrameWriter write() {
        return ds.write();
    }
}
