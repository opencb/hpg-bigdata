package org.opencb.hpg.bigdata.tools.spark;

import org.apache.commons.jexl2.UnifiedJEXL;
import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.tools.ant.taskdefs.Java;

import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;

/**
 * Created by jtarraga on 08/07/16.
 */
public class ParentDataset<T> {
    DataFrame df;
    SQLContext sqlContext;

    public ParentDataset() {
        df = null;
        sqlContext = null;
    }

    public void load(String filename, JavaSparkContext sparkContext) throws Exception {
        sqlContext = new SQLContext(sparkContext);
        if (filename.endsWith("avro")) {
            df = sqlContext.read().format("com.databricks.spark.avro").load(filename);
        } else if (filename.endsWith("json")) {
            df = sqlContext.read().json(filename);
        } else {
            throw new Exception("File type not supported yet !");
        }
    }

    public T agg(Column expr, Column... exprs) {
        df = df.agg(expr, exprs);
        return (T) this;
    }

    public T agg(Column expr, scala.collection.Seq<Column> exprs) {
        df = df.agg(expr, exprs);
        return (T) this;
    }

    public T agg(scala.collection.immutable.Map<java.lang.String,java.lang.String> exprs) {
        df = df.agg(exprs);
        return (T) this;
    }

    public T agg(java.util.Map<java.lang.String,java.lang.String> exprs) {
        df = df.agg(exprs);
        return (T) this;
    }

    public T agg(scala.Tuple2<java.lang.String,java.lang.String> aggExpr, scala.collection.Seq<scala.Tuple2<java.lang.String,java.lang.String>> aggExprs) {
        df = df.agg(aggExpr, aggExprs);
        return (T) this;
    }

    public T alias(java.lang.String alias)  {
        df = df.alias(alias);
        return (T) this;
    }

    public T alias(scala.Symbol alias) {
        df = df.alias(alias);
        return (T) this;
    }

    Column apply(java.lang.String colName) {
        return df.apply(colName);
    }

    public T as(java.lang.String alias) {
        df = df.as(alias);
        return (T) this;
    }

    public T as(scala.Symbol alias) {
        df = df.as(alias);
        return (T) this;
    }

    public T cache() {
        df = df.cache();
        return (T) this;
    }

    public T coalesce(int numPartitions) {
        df = df.coalesce(numPartitions);
        return (T) this;
    }

    Column col(java.lang.String colName) {
        return df.col(colName);
    }

    Row[] collect() {
        return df.collect();
    }

    java.util.List<Row> collectAsList() {
        return df.collectAsList();
    }

    protected int collectToPython() {
        return df.collectToPython();
    }

    java.lang.String[] columns() {
        return df.columns();
    }

    long count() {
        return df.count();
    }

    GroupedData cube(Column... cols) {
        return df.cube(cols);
    }

    GroupedData	cube(scala.collection.Seq<Column> cols) {
        return df.cube(cols);
    }

    GroupedData	cube(java.lang.String col1, scala.collection.Seq<java.lang.String> cols) {
        return df.cube(col1, cols);
    }

    GroupedData	cube(java.lang.String col1, java.lang.String... cols) {
        return df.cube(col1, cols);
    }

    public T describe(scala.collection.Seq<java.lang.String> cols) {
        df = df.describe(cols);
        return (T) this;
    }

    public T describe(java.lang.String... cols) {
        df = df.describe(cols);
        return (T) this;
    }

    public T distinct() {
        df = df.distinct();
        return (T) this;
    }

    public T drop(Column col) {
        df = df.drop(col);
        return (T) this;
    }

    public T drop(java.lang.String colName) {
        df = df.drop(colName);
        return (T) this;
    }

    public T dropDuplicates() {
        df = df.dropDuplicates();
        return (T) this;
    }

    public T dropDuplicates(scala.collection.Seq<java.lang.String> colNames) {
        df = df.dropDuplicates(colNames);
        return (T) this;
    }

    public T dropDuplicates(java.lang.String[] colNames) {
        df = df.dropDuplicates(colNames);
        return (T) this;
    }

    scala.Tuple2<java.lang.String,java.lang.String>[] dtypes() {
        return df.dtypes();
    }

    public T except(DataFrame other) {
        df = df.except(other);
        return (T) this;
    }

    void explain() {
        df.explain();
    }

    void explain(boolean extended) {
        df.explain(extended);
    }
/*
    public T explode(scala.collection.Seq<Column> input, scala.Function1<Row,scala.collection.TraversableOnce<A>> f, scala.reflect.api.TypeTags.TypeTag<A> evidence$2) {
        df = df.explode(input, );
        return (T) this;
    }

    <A,B> DataFrame 	explode(java.lang.String inputColumn, java.lang.String outputColumn, scala.Function1<A,scala.collection.TraversableOnce<B>> f, scala.reflect.api.TypeTags.TypeTag<B> evidence$3)
*/
    public T filter(Column condition) {
        df = df.filter(condition);
        return (T) this;
    }

    public T filter(java.lang.String conditionExpr) {
        df = df.filter(conditionExpr);
        return (T) this;
    }

    Row first() {
        return df.first();
    }

    void foreach(scala.Function1<Row,scala.runtime.BoxedUnit> f) {
        df.foreach(f);
    }

    void foreachPartition(scala.Function1<scala.collection.Iterator<Row>,scala.runtime.BoxedUnit> f) {
        df.foreachPartition(f);
    }

    GroupedData	groupBy(Column... cols) {
        return df.groupBy(cols);
    }

    GroupedData	groupBy(scala.collection.Seq<Column> cols) {
        return df.groupBy(cols);
    }

    GroupedData	groupBy(java.lang.String col1, scala.collection.Seq<java.lang.String> cols) {
        return df.groupBy(col1, cols);
    }

    GroupedData	groupBy(java.lang.String col1, java.lang.String... cols) {
        return df.groupBy(col1, cols);
    }

    Row	head() {
        return df.head();
    }

    Row[] head(int n) {
        return df.head(n);
    }

    java.lang.String[] inputFiles() {
        return df.inputFiles();
    }

    public T intersect(DataFrame other) {
        df = df.intersect(other);
        return (T) this;
    }

    boolean	isLocal() {
        return df.isLocal();
    }

    JavaRDD<Row> javaRDD() {
        return df.javaRDD();
    }

    protected JavaRDD<byte[]> javaToPython() {
        return df.javaToPython();
    }

    public T join(DataFrame right) {
        df = df.join(right);
        return (T) this;
    }

    public T join(DataFrame right, Column joinExprs) {
        df = df.join(right, joinExprs);
        return (T) this;
    }

    public T join(DataFrame right, Column joinExprs, java.lang.String joinType) {
        df = df.join(right, joinExprs, joinType);
        return (T) this;
    }

    public T join(DataFrame right, scala.collection.Seq<java.lang.String> usingColumns) {
        df = df.join(right, usingColumns);
        return (T) this;
    }

    public T join(DataFrame right, scala.collection.Seq<java.lang.String> usingColumns, java.lang.String joinType) {
        df = df.join(right, usingColumns, joinType);
        return (T) this;
    }

    public T join(DataFrame right, java.lang.String usingColumn) {
        df = df.join(right, usingColumn);
        return (T) this;
    }

    public T limit(int n) {
        df = df.limit(n);
        return (T) this;
    }

    protected org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan() {
        return df.logicalPlan();
    }

    <R>RDD<R> map(scala.Function1<Row,R> f, scala.reflect.ClassTag<R> evidence$4) {
        return df.map(f, evidence$4);
    }

    <R>RDD<R> mapPartitions(scala.Function1<scala.collection.Iterator<Row>,scala.collection.Iterator<R>> f, scala.reflect.ClassTag<R> evidence$6) {
        return df.mapPartitions(f, evidence$6);
    }

    DataFrameNaFunctions na() {
        return df.na();
    }

    protected scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> numericColumns() {
        return df.numericColumns();
    }

    public T orderBy(Column... sortExprs) {
        df = df.orderBy(sortExprs);
        return (T) this;
    }

    public T orderBy(scala.collection.Seq<Column> sortExprs) {
        df = df.orderBy(sortExprs);
        return (T) this;
    }

    public T orderBy(java.lang.String sortCol, scala.collection.Seq<java.lang.String> sortCols) {
        df = df.orderBy(sortCol, sortCols);
        return (T) this;
    }

    public T orderBy(java.lang.String sortCol, java.lang.String... sortCols) {
        df = df.orderBy(sortCol, sortCols);
        return (T) this;
    }

    public T persist() {
        df = df.persist();
        return (T) this;
    }

    public T persist(StorageLevel newLevel) {
        df = df.persist(newLevel);
        return (T) this;
    }

    void printSchema() {
        df.printSchema();
    }

    /*
    public T[] randomSplit(double[] weights) {
        df = df.randomSplit();
        return (T) this;
    }
    public T[] randomSplit(double[] weights, long seed)
    */

    RDD<Row> rdd() {
        return df.rdd();
    }

    void registerTempTable(java.lang.String tableName) {
        df.registerTempTable(tableName);
    }

    public T repartition(Column... partitionExprs) {
        df = df.repartition(partitionExprs);
        return (T) this;
    }

    public T repartition(int numPartitions) {
        df = df.repartition(numPartitions);
        return (T) this;
    }

    public T repartition(int numPartitions, Column... partitionExprs) {
        df = df.repartition(numPartitions, partitionExprs);
        return (T) this;
    }

    public T repartition(int numPartitions, scala.collection.Seq<Column> partitionExprs) {
        df = df.repartition(numPartitions, partitionExprs);
        return (T) this;
    }

    public T repartition(scala.collection.Seq<Column> partitionExprs) {
        df = df.repartition(partitionExprs);
        return (T) this;
    }

    protected org.apache.spark.sql.catalyst.expressions.NamedExpression resolve(java.lang.String colName) {
        return df.resolve(colName);
    }

    GroupedData rollup(Column... cols) {
        return df.rollup(cols);
    }

    GroupedData rollup(scala.collection.Seq<Column> cols) {
        return df.rollup(cols);
    }

    GroupedData rollup(java.lang.String col1, scala.collection.Seq<java.lang.String> cols) {
        return df.rollup(col1, cols);
    }

    GroupedData rollup(java.lang.String col1, java.lang.String... cols) {
        return df.rollup(col1, cols);
    }

    public T sample(boolean withReplacement, double fraction) {
        df = df.sample(withReplacement, fraction);
        return (T) this;
    }

    public T sample(boolean withReplacement, double fraction, long seed) {
        df = df.sample(withReplacement, fraction, seed);
        return (T) this;
    }

    StructType schema() {
        return df.schema();
    }

    public T select(Column... cols) {
        df = df.select(cols);
        return (T) this;
    }

    public T select(scala.collection.Seq<Column> cols) {
        df = df.select(cols);
        return (T) this;
    }

    public T select(java.lang.String col, scala.collection.Seq<java.lang.String> cols) {
        df = df.select(col, cols);
        return (T) this;
    }

    public T select(java.lang.String col, java.lang.String... cols) {
        df = df.select(col, cols);
        return (T) this;
    }

    public T selectExpr(scala.collection.Seq<java.lang.String> exprs) {
        df = df.selectExpr(exprs);
        return (T) this;
    }

    public T selectExpr(java.lang.String... exprs) {
        df = df.selectExpr(exprs);
        return (T) this;
    }

    void show() {
        df.show();
    }

    void show(boolean truncate) {
        df.show(truncate);
    }

    void show(int numRows) {
        df.show(numRows);
    }

    void show(int numRows, boolean truncate) {
        df.show(numRows, truncate);
    }

    public T sort(Column... sortExprs) {
        df = df.sort(sortExprs);
        return (T) this;
    }

    public T sort(scala.collection.Seq<Column> sortExprs) {
        df = df.sort(sortExprs);
        return (T) this;
    }

    public T sort(java.lang.String sortCol, scala.collection.Seq<java.lang.String> sortCols) {
        df = df.sort(sortCol, sortCols);
        return (T) this;
    }

    public T sort(java.lang.String sortCol, java.lang.String... sortCols) {
        df = df.sort(sortCol, sortCols);
        return (T) this;
    }

    public T sortWithinPartitions(Column... sortExprs) {
        df = df.sortWithinPartitions(sortExprs);
        return (T) this;
    }

    public T sortWithinPartitions(scala.collection.Seq<Column> sortExprs) {
        df = df.sortWithinPartitions(sortExprs);
        return (T) this;
    }

    public T sortWithinPartitions(java.lang.String sortCol, scala.collection.Seq<java.lang.String> sortCols) {
        df = df.sortWithinPartitions(sortCol, sortCols);
        return (T) this;
    }

    public T sortWithinPartitions(java.lang.String sortCol, java.lang.String... sortCols) {
        df = df.sortWithinPartitions(sortCol, sortCols);
        return (T) this;
    }

    SQLContext sqlContext() {
        return df.sqlContext();
    }

    DataFrameStatFunctions stat() {
        return df.stat();
    }

    Row[] take(int n) {
        return df.take(n);
    }

    java.util.List<Row> takeAsList(int n) {
        return df.takeAsList(n);
    }

    public T toDF() {
        df = df.toDF();
        return (T) this;
    }

    public T toDF(scala.collection.Seq<java.lang.String> colNames) {
        df = df.toDF(colNames);
        return (T) this;
    }

    public T toDF(java.lang.String... colNames) {
        df = df.toDF(colNames);
        return (T) this;
    }

    JavaRDD<Row> toJavaRDD() {
        return df.toJavaRDD();
    }

    RDD<java.lang.String> toJSON() {
        return df.toJSON();
    }

//    <U> DataFrame transform(scala.Function1<DataFrame,DataFrame> t)

    public T unionAll(DataFrame other) {
        df = df.unionAll(other);
        return (T) this;
    }

    public T unpersist() {
        df = df.unpersist();
        return (T) this;
    }

    public T unpersist(boolean blocking) {
        df = df.unpersist(blocking);
        return (T) this;
    }

    public T where(Column condition) {
        df = df.where(condition);
        return (T) this;
    }

    public T where(java.lang.String conditionExpr) {
        df = df.where(conditionExpr);
        return (T) this;
    }

    public T withColumn(java.lang.String colName, Column col) {
        df = df.withColumn(colName, col);
        return (T) this;
    }

    public T withColumnRenamed(java.lang.String existingName, java.lang.String newName) {
        df = df.withColumnRenamed(existingName, newName);
        return (T) this;
    }

    public DataFrameWriter write() {
        return df.write();
    }
}
