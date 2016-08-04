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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Symbol;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;


/**
 * Created by imedina on 04/08/16.
 */
public class ParentDataset<T> {

    protected DataFrame df;
    protected SQLContext sqlContext;

    public ParentDataset() {
        df = null;
        sqlContext = null;
    }

    public void load(String filename, JavaSparkContext sparkContext) throws Exception {
        sqlContext = new SQLContext(sparkContext);

        if (StringUtils.endsWithAny(filename, "avro", "avro.gz", "avro.sz")) {
            df = sqlContext.read().format("com.databricks.spark.avro").load(filename);
        } else if (StringUtils.endsWithAny(filename, "json", "json.gz")) {
            df = sqlContext.read().json(filename);
        } else {
            df = sqlContext.read().load(filename);
        }
    }

    public ParentDataset<T> agg(Column expr, Column... exprs) {
        df = df.agg(expr, exprs);
        return this;
    }

    public ParentDataset<T> agg(Column expr, Seq<Column> exprs) {
        df = df.agg(expr, exprs);
        return this;
    }

    public ParentDataset<T> agg(scala.collection.immutable.Map<String, String> exprs) {
        df = df.agg(exprs);
        return this;
    }

    public ParentDataset<T> agg(Map<String, String> exprs) {
        df = df.agg(exprs);
        return this;
    }

    public ParentDataset<T> agg(Tuple2<String, String> aggExpr, Seq<Tuple2<String, String>> aggExprs) {
        df = df.agg(aggExpr, aggExprs);
        return this;
    }

    public ParentDataset<T> alias(String alias)  {
        df = df.alias(alias);
        return this;
    }

    public ParentDataset<T> alias(Symbol alias) {
        df = df.alias(alias);
        return this;
    }

    public Column apply(String colName) {
        return df.apply(colName);
    }

    public ParentDataset<T> as(String alias) {
        df = df.as(alias);
        return this;
    }

    public ParentDataset<T> as(scala.Symbol alias) {
        df = df.as(alias);
        return this;
    }

    public ParentDataset<T> cache() {
        df = df.cache();
        return this;
    }

    public ParentDataset<T> coalesce(int numPartitions) {
        df = df.coalesce(numPartitions);
        return this;
    }

    public Column col(String colName) {
        return df.col(colName);
    }

    public Row[] collect() {
        return df.collect();
    }

    public List<Row> collectAsList() {
        return df.collectAsList();
    }

    protected int collectToPython() {
        return df.collectToPython();
    }

    public String[] columns() {
        return df.columns();
    }

    long count() {
        return df.count();
    }

    public GroupedData cube(Column... cols) {
        return df.cube(cols);
    }

    public GroupedData cube(Seq<Column> cols) {
        return df.cube(cols);
    }

    public GroupedData cube(String col1, Seq<String> cols) {
        return df.cube(col1, cols);
    }

    public GroupedData cube(String col1, String... cols) {
        return df.cube(col1, cols);
    }

    public ParentDataset<T> describe(Seq<String> cols) {
        df = df.describe(cols);
        return this;
    }

    public ParentDataset<T> describe(String... cols) {
        df = df.describe(cols);
        return this;
    }

    public ParentDataset<T> distinct() {
        df = df.distinct();
        return this;
    }

    public ParentDataset<T> drop(Column col) {
        df = df.drop(col);
        return this;
    }

    public ParentDataset<T> drop(String colName) {
        df = df.drop(colName);
        return this;
    }

    public ParentDataset<T> dropDuplicates() {
        df = df.dropDuplicates();
        return this;
    }

    public ParentDataset<T> dropDuplicates(Seq<String> colNames) {
        df = df.dropDuplicates(colNames);
        return this;
    }

    public ParentDataset<T> dropDuplicates(String[] colNames) {
        df = df.dropDuplicates(colNames);
        return this;
    }

    public Tuple2<String, String>[] dtypes() {
        return df.dtypes();
    }

    public ParentDataset<T> except(DataFrame other) {
        df = df.except(other);
        return this;
    }

    public void explain() {
        df.explain();
    }

    public void explain(boolean extended) {
        df.explain(extended);
    }

//    public ParentDataset<T> explode(Seq<Column> input, scala.Function1<Row,TraversableOnce<A>> f,
//                                    scala.reflect.api.TypeTags.TypeTag<A> evidence) {
//        df = df.explode(input, evidence);
//        return this;
//    }
//    public <A,B> DataFrame explode(String inputColumn, String outputColumn, scala.Function1<A,TraversableOnce<B>> f,
//                            scala.reflect.api.TypeTags.TypeTag<B> evidence)


    public ParentDataset<T> filter(Column condition) {
        df = df.filter(condition);
        return this;
    }

    public ParentDataset<T> filter(String conditionExpr) {
        df = df.filter(conditionExpr);
        return this;
    }

    public Row first() {
        return df.first();
    }

    public void foreach(scala.Function1<Row, scala.runtime.BoxedUnit> f) {
        df.foreach(f);
    }

    public void foreachPartition(scala.Function1<scala.collection.Iterator<Row>, scala.runtime.BoxedUnit> f) {
        df.foreachPartition(f);
    }

    public GroupedData groupBy(Column... cols) {
        return df.groupBy(cols);
    }

    public GroupedData groupBy(Seq<Column> cols) {
        return df.groupBy(cols);
    }

    public GroupedData groupBy(String col1, Seq<String> cols) {
        return df.groupBy(col1, cols);
    }

    public GroupedData groupBy(String col1, String... cols) {
        return df.groupBy(col1, cols);
    }

    public Row head() {
        return df.head();
    }

    public Row[] head(int n) {
        return df.head(n);
    }

    public String[] inputFiles() {
        return df.inputFiles();
    }

    public ParentDataset<T> intersect(DataFrame other) {
        df = df.intersect(other);
        return this;
    }

    boolean isLocal() {
        return df.isLocal();
    }

    public JavaRDD<Row> javaRDD() {
        return df.javaRDD();
    }

    protected JavaRDD<byte[]> javaToPython() {
        return df.javaToPython();
    }

    public ParentDataset<T> join(DataFrame right) {
        df = df.join(right);
        return this;
    }

    public ParentDataset<T> join(DataFrame right, Column joinExprs) {
        df = df.join(right, joinExprs);
        return this;
    }

    public ParentDataset<T> join(DataFrame right, Column joinExprs, String joinType) {
        df = df.join(right, joinExprs, joinType);
        return this;
    }

    public ParentDataset<T> join(DataFrame right, Seq<String> usingColumns) {
        df = df.join(right, usingColumns);
        return this;
    }

    public ParentDataset<T> join(DataFrame right, Seq<String> usingColumns, String joinType) {
        df = df.join(right, usingColumns, joinType);
        return this;
    }

    public ParentDataset<T> join(DataFrame right, String usingColumn) {
        df = df.join(right, usingColumn);
        return this;
    }

    public ParentDataset<T> limit(int n) {
        df = df.limit(n);
        return this;
    }

    protected org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan() {
        return df.logicalPlan();
    }

    public <R> RDD<R> map(scala.Function1<Row, R> f, scala.reflect.ClassTag<R> evidence) {
        return df.map(f, evidence);
    }

    public <R> RDD<R> mapPartitions(scala.Function1<scala.collection.Iterator<Row>, scala.collection.Iterator<R>> f,
                                    scala.reflect.ClassTag<R> evidence) {
        return df.mapPartitions(f, evidence);
    }

    public DataFrameNaFunctions na() {
        return df.na();
    }

    protected Seq<org.apache.spark.sql.catalyst.expressions.Expression> numericColumns() {
        return df.numericColumns();
    }

    public ParentDataset<T> orderBy(Column... sortExprs) {
        df = df.orderBy(sortExprs);
        return this;
    }

    public ParentDataset<T> orderBy(Seq<Column> sortExprs) {
        df = df.orderBy(sortExprs);
        return this;
    }

    public ParentDataset<T> orderBy(String sortCol, Seq<String> sortCols) {
        df = df.orderBy(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> orderBy(String sortCol, String... sortCols) {
        df = df.orderBy(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> persist() {
        df = df.persist();
        return this;
    }

    public ParentDataset<T> persist(StorageLevel newLevel) {
        df = df.persist(newLevel);
        return this;
    }

    public void printSchema() {
        df.printSchema();
    }

    /*
    public T[] randomSplit(double[] weights) {
        df = df.randomSplit();
        return this;
    }
    public T[] randomSplit(double[] weights, long seed)
    */

    public RDD<Row> rdd() {
        return df.rdd();
    }

    public void registerTempTable(String tableName) {
        df.registerTempTable(tableName);
    }

    public ParentDataset<T> repartition(Column... partitionExprs) {
        df = df.repartition(partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions) {
        df = df.repartition(numPartitions);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions, Column... partitionExprs) {
        df = df.repartition(numPartitions, partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(int numPartitions, Seq<Column> partitionExprs) {
        df = df.repartition(numPartitions, partitionExprs);
        return this;
    }

    public ParentDataset<T> repartition(Seq<Column> partitionExprs) {
        df = df.repartition(partitionExprs);
        return this;
    }

    protected org.apache.spark.sql.catalyst.expressions.NamedExpression resolve(String colName) {
        return df.resolve(colName);
    }

    public GroupedData rollup(Column... cols) {
        return df.rollup(cols);
    }

    public GroupedData rollup(Seq<Column> cols) {
        return df.rollup(cols);
    }

    public GroupedData rollup(String col1, Seq<String> cols) {
        return df.rollup(col1, cols);
    }

    public GroupedData rollup(String col1, String... cols) {
        return df.rollup(col1, cols);
    }

    public ParentDataset<T> sample(boolean withReplacement, double fraction) {
        df = df.sample(withReplacement, fraction);
        return this;
    }

    public ParentDataset<T> sample(boolean withReplacement, double fraction, long seed) {
        df = df.sample(withReplacement, fraction, seed);
        return this;
    }

    public StructType schema() {
        return df.schema();
    }

    public ParentDataset<T> select(Column... cols) {
        df = df.select(cols);
        return this;
    }

    public ParentDataset<T> select(Seq<Column> cols) {
        df = df.select(cols);
        return this;
    }

    public ParentDataset<T> select(String col, Seq<String> cols) {
        df = df.select(col, cols);
        return this;
    }

    public ParentDataset<T> select(String col, String... cols) {
        df = df.select(col, cols);
        return this;
    }

    public ParentDataset<T> selectExpr(Seq<String> exprs) {
        df = df.selectExpr(exprs);
        return this;
    }

    public ParentDataset<T> selectExpr(String... exprs) {
        df = df.selectExpr(exprs);
        return this;
    }

    public void show() {
        df.show();
    }

    public void show(boolean truncate) {
        df.show(truncate);
    }

    public void show(int numRows) {
        df.show(numRows);
    }

    public void show(int numRows, boolean truncate) {
        df.show(numRows, truncate);
    }

    public ParentDataset<T> sort(Column... sortExprs) {
        df = df.sort(sortExprs);
        return this;
    }

    public ParentDataset<T> sort(Seq<Column> sortExprs) {
        df = df.sort(sortExprs);
        return this;
    }

    public ParentDataset<T> sort(String sortCol, Seq<String> sortCols) {
        df = df.sort(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sort(String sortCol, String... sortCols) {
        df = df.sort(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(Column... sortExprs) {
        df = df.sortWithinPartitions(sortExprs);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(Seq<Column> sortExprs) {
        df = df.sortWithinPartitions(sortExprs);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(String sortCol, Seq<String> sortCols) {
        df = df.sortWithinPartitions(sortCol, sortCols);
        return this;
    }

    public ParentDataset<T> sortWithinPartitions(String sortCol, String... sortCols) {
        df = df.sortWithinPartitions(sortCol, sortCols);
        return this;
    }

    public SQLContext sqlContext() {
        return df.sqlContext();
    }

    public DataFrameStatFunctions stat() {
        return df.stat();
    }

    public Row[] take(int n) {
        return df.take(n);
    }

    public List<Row> takeAsList(int n) {
        return df.takeAsList(n);
    }

    public ParentDataset<T> toDF() {
        df = df.toDF();
        return this;
    }

    public ParentDataset<T> toDF(Seq<String> colNames) {
        df = df.toDF(colNames);
        return this;
    }

    public ParentDataset<T> toDF(String... colNames) {
        df = df.toDF(colNames);
        return this;
    }

    public JavaRDD<Row> toJavaRDD() {
        return df.toJavaRDD();
    }

    public RDD<String> toJSON() {
        return df.toJSON();
    }

//    <U> DataFrame transform(scala.Function1<DataFrame,DataFrame> t)

    public ParentDataset<T> unionAll(DataFrame other) {
        df = df.unionAll(other);
        return this;
    }

    public ParentDataset<T> unpersist() {
        df = df.unpersist();
        return this;
    }

    public ParentDataset<T> unpersist(boolean blocking) {
        df = df.unpersist(blocking);
        return this;
    }

    public ParentDataset<T> where(Column condition) {
        df = df.where(condition);
        return this;
    }

    public ParentDataset<T> where(String conditionExpr) {
        df = df.where(conditionExpr);
        return this;
    }

    public ParentDataset<T> withColumn(String colName, Column col) {
        df = df.withColumn(colName, col);
        return this;
    }

    public ParentDataset<T> withColumnRenamed(String existingName, String newName) {
        df = df.withColumnRenamed(existingName, newName);
        return this;
    }

    public DataFrameWriter write() {
        return df.write();
    }
}
