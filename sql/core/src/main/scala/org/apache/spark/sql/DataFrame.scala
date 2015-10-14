/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql

import java.io.CharArrayWriter
import java.util.Properties

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonFactory
import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, ResolvedStar, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, _}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, SqlParser}
import org.apache.spark.sql.execution.{EvaluatePython, ExplainCommand, LogicalRDD}
import org.apache.spark.sql.json.JacksonGenerator
import org.apache.spark.sql.sources.CreateTableUsingAsSelect
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils


private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}


/**
 * :: Experimental ::
 * A distributed collection of data organized into named columns.
 *
 * A [[DataFrame]] is equivalent to a relational table in Spark SQL. The following example creates
 * a [[DataFrame]] by pointing Spark SQL to a Parquet data set.
 * {{{
 *   val people = sqlContext.read.parquet("...")  // in Scala
 *   DataFrame people = sqlContext.read().parquet("...")  // in Java
 * }}}
 *
 * Once created, it can be manipulated using the various domain-specific-language (DSL) functions
 * defined in: [[DataFrame]] (this class), [[Column]], and [[functions]].
 *
 * To select a column from the data frame, use `apply` method in Scala and `col` in Java.
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.col("age")  // in Java
 * }}}
 *
 * Note that the [[Column]] type can also be manipulated through its various functions.
 * {{{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 *   people.col("age").plus(10);  // in Java
 * }}}
 *
 * A more concrete example in Scala:
 * {{{
 *   // To create DataFrame using SQLContext
 *   val people = sqlContext.read.parquet("...")
 *   val department = sqlContext.read.parquet("...")
 *
 *   people.filter("age > 30")
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), "gender")
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 *
 * and in Java:
 * {{{
 *   // To create DataFrame using SQLContext
 *   DataFrame people = sqlContext.read().parquet("...");
 *   DataFrame department = sqlContext.read().parquet("...");
 *
 *   people.filter("age".gt(30))
 *     .join(department, people.col("deptId").equalTo(department("id")))
 *     .groupBy(department.col("name"), "gender")
 *     .agg(avg(people.col("salary")), max(people.col("age")));
 * }}}
 *
 * @groupname basic Basic DataFrame functions
 * @groupname dfops Language Integrated Queries
 * @groupname rdd RDD Operations
 * @groupname output Output Operations
 * @groupname action Actions
 * @since 1.3.0
 */
// TODO: Improve documentation.
@Experimental
class DataFrame private[sql](
    @transient val sqlContext: SQLContext,
    @DeveloperApi @transient val queryExecution: SQLContext#QueryExecution)
  extends RDDApi[Row] with Serializable {

  /**
   * A constructor that automatically analyzes the logical plan.
   *
   * This reports error eagerly as the [[DataFrame]] is constructed, unless
   * [[SQLConf.dataFrameEagerAnalysis]] is turned off.
   */
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }

  @transient protected[sql] val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command |
         _: InsertIntoTable |
         _: CreateTableUsingAsSelect |
         _: WriteToFile =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.analyzed
  }

  /**
   * An implicit conversion function internal to this class for us to avoid doing
   * "new DataFrame(...)" everywhere.
   */
  @inline private implicit def logicalPlanToDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

  protected[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolveQuoted(colName, sqlContext.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
    }
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sqlContext.analyzer.resolver).get
    }
  }

  /**
   * Internal API for Python
   * @param numRows Number of rows to show
   */
  private[sql] def showString(numRows: Int): String = {
    val sb = new StringBuilder
    val data = take(numRows)
    val numCols = schema.fieldNames.length

    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = if (cell == null) "null" else cell.toString
        if (str.length > 20) str.substring(0, 17) + "..." else str
      }: Seq[String]
    }

    // Compute the width of each column
    val colWidths = Array.fill(numCols)(0)
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      StringUtils.leftPad(cell.toString, colWidths(i))
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        StringUtils.leftPad(cell.toString, colWidths(i))
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)
    sb.toString()
  }

  override def toString: String = {
    try {
      schema.map(f => s"${f.name}: ${f.dataType.simpleString}").mkString("[", ", ", "]")
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /**
   * Returns the object itself.
   * @group basic
   * @since 1.3.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = this

  /**
   * Returns a new [[DataFrame]] with columns renamed. This can be quite convenient in conversion
   * from a RDD of tuples into a [[DataFrame]] with meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name _1 and _2
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   * @group basic
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        s"Old column names (${schema.size}): " + schema.fields.map(_.name).mkString(", ") + "\n" +
        s"New column names (${colNames.size}): " + colNames.mkString(", "))

    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
      Column(oldAttribute).as(newName)
    }
    select(newCols : _*)
  }

  /**
   * Returns the schema of this [[DataFrame]].
   * @group basic
   * @since 1.3.0
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Returns all column names and their data types as an array.
   * @group basic
   * @since 1.3.0
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
   * @group basic
   * @since 1.3.0
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Prints the schema to the console in a nice tree format.
   * @group basic
   * @since 1.3.0
   */
  def printSchema(): Unit = println(schema.treeString)

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
   * @group basic
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    ExplainCommand(
      queryExecution.logical,
      extended = extended).queryExecution.executedPlan.executeCollect().map {
      r => println(r.getString(0))
    }
  }

  /**
   * Only prints the physical plan to the console for debugging purposes.
   * @group basic
   * @since 1.3.0
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
   * (without any Spark executors).
   * @group basic
   * @since 1.3.0
   */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  /**
   * Displays the [[DataFrame]] in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   *
   * @group action
   * @since 1.3.0
   */
  def show(numRows: Int): Unit = println(showString(numRows))

  /**
   * Displays the top 20 rows of [[DataFrame]] in a tabular form.
   * @group action
   * @since 1.3.0
   */
  def show(): Unit = show(20)

  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
   * {{{
   *   // Dropping rows containing any null values.
   *   df.na.drop()
   * }}}
   *
   * @group dfops
   * @since 1.3.1
   */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(this)

  /**
   * Returns a [[DataFrameStatFunctions]] for working statistic functions support.
   * {{{
   *   // Finding frequent items in column with name 'a'.
   *   df.stat.freqItems(Seq("a"))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(this)

  /**
   * Cartesian join with another [[DataFrame]].
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  /**
   * Inner equi-join with another [[DataFrame]] using the given column.
   *
   * Different from other join functions, the join column will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the column "user_id"
   *   df1.join(df2, "user_id")
   * }}}
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumn Name of the column to join on. This column must exist on both sides.
   * @group dfops
   * @since 1.4.0
   */
  def join(right: DataFrame, usingColumn: String): DataFrame = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sqlContext.executePlan(
      Join(logicalPlan, right.logicalPlan, joinType = Inner, None)).analyzed.asInstanceOf[Join]

    // Project only one of the join column.
    val joinedCol = joined.right.resolve(usingColumn)
    Project(
      joined.output.filterNot(_ == joinedCol),
      Join(
        joined.left,
        joined.right,
        joinType = Inner,
        Some(catalyst.expressions.EqualTo(
          joined.left.resolve(usingColumn),
          joined.right.resolve(usingColumn))))
    )
  }

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

  /**
   * Join with another [[DataFrame]], using the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df1.join(df2, $"df1Key" === $"df2Key", "outer")
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df1.join(df2, col("df1Key").equalTo(col("df2Key")), "outer");
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
    // Note that in this function, we introduce a hack in the case of self-join to automatically
    // resolve ambiguous join conditions into ones that might make sense [SPARK-6231].
    // Consider this case: df.join(df, df("key") === df("key"))
    // Since df("key") === df("key") is a trivially true condition, this actually becomes a
    // cartesian join. However, most likely users expect to perform a self join using "key".
    // With that assumption, this hack turns the trivially true condition into equality on join
    // keys that are resolved to both sides.

    // Trigger analysis so in the case of self-join, the analyzer will clone the plan.
    // After the cloning, left and right side will have distinct expression ids.
    val plan = Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr))
      .queryExecution.analyzed.asInstanceOf[Join]

    // If auto self join alias is disabled, return the plan.
    if (!sqlContext.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return plan
    }

    // If left/right have no output set intersection, return the plan.
    val lanalyzed = this.logicalPlan.queryExecution.analyzed
    val ranalyzed = right.logicalPlan.queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return plan
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.
    val cond = plan.condition.map { _.transform {
      case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
        catalyst.expressions.EqualTo(plan.left.resolve(a.name), plan.right.resolve(b.name))
    }}
    plan.copy(condition = cond)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   df.sort("sortcol")
   *   df.sort($"sortcol")
   *   df.sort($"sortcol".asc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): DataFrame = {
    sort((sortCol +: sortCols).map(apply) : _*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): DataFrame = sort(sortCol, sortCols : _*)

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): DataFrame = sort(sortExprs : _*)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * @group dfops
   * @since 1.3.0
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects column based on the column name and return it as a [[Column]].
   * @group dfops
   * @since 1.3.0
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(schema.fieldNames.map(resolve)))
    case _ =>
      val expr = resolve(colName)
      Column(expr)
  }

  /**
   * Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   * @since 1.3.0
   */
  def as(alias: String): DataFrame = Subquery(alias, logicalPlan)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   * @since 1.3.0
   */
  def as(alias: Symbol): DataFrame = as(alias.name)

  /**
   * Selects a set of column based expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = {
    val namedExpressions = cols.map {
      case Column(expr: NamedExpression) => expr
      // Leave an unaliased explode with an empty list of names since the analzyer will generate the
      // correct defaults after the nested expression's type has been resolved.
      case Column(explode: Explode) => MultiAlias(explode, Nil)
      case Column(expr: Expression) => Alias(expr, expr.prettyString)()
    }
    // When user continuously call `select`, speed up analysis by collapsing `Project`
    import org.apache.spark.sql.catalyst.optimizer.ProjectCollapsing
    Project(namedExpressions.toSeq, ProjectCollapsing(logicalPlan))
  }

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   df.select("colA", "colB")
   *   df.select($"colA", $"colB")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) : _*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(new SqlParser().parseExpression(expr))
    }: _*)
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def filter(condition: Column): DataFrame = Filter(condition.expr, logicalPlan)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def filter(conditionExpr: String): DataFrame = {
    filter(Column(new SqlParser().parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def where(condition: Column): DataFrame = filter(condition)

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): GroupedData = {
    GroupedData(this, cols.map(_.expr), GroupedData.GroupByType)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(cols: Column*): GroupedData = {
    GroupedData(this, cols.map(_.expr), GroupedData.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(cols: Column*): GroupedData = GroupedData(this, cols.map(_.expr), GroupedData.CubeType)

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of groupBy that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy("department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.GroupByType)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of rollup that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup("department", "group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of cube that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube("department", "group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.CubeType)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg("age" -> "max", "salary" -> "avg")
   *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs : _*)

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
   * @group dfops
   * @since 1.3.0
   */
  def limit(n: Int): DataFrame = Limit(Literal(n), logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
   * This is equivalent to `UNION ALL` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def unionAll(other: DataFrame): DataFrame = Union(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
   * This is equivalent to `INTERSECT` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def intersect(other: DataFrame): DataFrame = Intersect(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def except(other: DataFrame): DataFrame = Except(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.3.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    Sample(0.0, fraction, withReplacement, seed, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @group dfops
   * @since 1.3.0
   */
  def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[DataFrame] = {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new DataFrame(sqlContext, Sample(x(0), x(1), false, seed, logicalPlan))
    }.toArray
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double]): Array[DataFrame] = {
    randomSplit(weights, Utils.random.nextLong)
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights. Provided for the Python Api.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[DataFrame] = {
    randomSplit(weights.toArray, seed)
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where each row has been expanded to zero or more
   * rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. The columns of
   * the input row are implicitly joined with each row that is output by the function.
   *
   * The following example uses this function to count the number of books which contain
   * a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val df: RDD[Book]
   *
   *   case class Word(word: String)
   *   val allWords = df.explode('words) {
   *     case Row(words: String) => words.split(" ").map(Word(_))
   *   }
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]

    val elementTypes = schema.toAttributes.map { attr => (attr.dataType, attr.nullable) }
    val names = schema.toAttributes.map(_.name)

    val rowFunction =
      f.andThen(_.map(CatalystTypeConverters.convertToCatalyst(_, schema).asInstanceOf[Row]))
    val generator = UserDefinedGenerator(elementTypes, rowFunction, input.map(_.expr))

    Generate(generator, join = true, outer = false,
      qualifier = None, names.map(UnresolvedAttribute(_)), logicalPlan)
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where a single column has been expanded to zero
   * or more rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * {{{
   *   df.explode("words", "word"){words: String => words.split(" ")}
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementTypes = attributes.map { attr => (attr.dataType, attr.nullable) }
    val names = attributes.map(_.name)

    def rowFunction(row: Row): TraversableOnce[Row] = {
      f(row(0).asInstanceOf[A]).map(o => Row(CatalystTypeConverters.convertToCatalyst(o, dataType)))
    }
    val generator = UserDefinedGenerator(elementTypes, rowFunction, apply(inputColumn).expr :: Nil)

    Generate(generator, join = true, outer = false,
      qualifier = None, names.map(UnresolvedAttribute(_)), logicalPlan)
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column.
   * @group dfops
   * @since 1.3.0
   */
  def withColumn(colName: String, col: Column): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val replaced = schema.exists(f => resolver(f.name, colName))
    if (replaced) {
      val colNames = schema.map { field =>
        val name = field.name
        if (resolver(name, colName)) col.as(colName) else Column(name)
      }
      select(colNames : _*)
    } else {
      select(Column("*"), col.as(colName))
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
   * @group dfops
   * @since 1.3.0
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val shouldRename = schema.exists(f => resolver(f.name, existingName))
    if (shouldRename) {
      val colNames = schema.map { field =>
        val name = field.name
        if (resolver(name, existingName)) Column(name).as(newName) else Column(name)
      }
      select(colNames : _*)
    } else {
      this
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This is a no-op if schema doesn't contain column name.
   * @group dfops
   * @since 1.4.0
   */
  def drop(colName: String): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val shouldDrop = schema.exists(f => resolver(f.name, colName))
    if (shouldDrop) {
      val colsAfterDrop = schema.filter { field =>
        val name = field.name
        !resolver(name, colName)
      }.map(f => Column(f.name))
      select(colsAfterDrop : _*)
    } else {
      this
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
   * This version of drop accepts a Column rather than a name.
   * This is a no-op if the DataFrame doesn't have a column
   * with an equivalent expression.
   * @group dfops
   * @since 1.4.1
   */
  def drop(col: Column): DataFrame = {
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      attr != col.expr
    }.map(attr => Column(attr))
    select(colsAfterDrop : _*)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
   * This is an alias for `distinct`.
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(): DataFrame = dropDuplicates(this.columns)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Seq[String]): DataFrame = {
    val groupCols = colNames.map(resolve)
    val groupColExprIds = groupCols.map(_.exprId)
    val aggCols = logicalPlan.output.map { attr =>
      if (groupColExprIds.contains(attr.exprId)) {
        attr
      } else {
        Alias(First(attr), attr.name)()
      }
    }
    Aggregate(groupCols, aggCols, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Array[String]): DataFrame = dropDuplicates(colNames.toSeq)

  /**
   * Computes statistics for numeric columns, including count, mean, stddev, min, and max.
   * If no columns are given, this function computes statistics for all numerical columns.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]]. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   df.describe("age", "height").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // max     92.0  192.0
   * }}}
   *
   * @group action
   * @since 1.3.1
   */
  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = {

    // TODO: Add stddev as an expression, and remove it from here.
    def stddevExpr(expr: Expression): Expression =
      Sqrt(Subtract(Average(Multiply(expr, expr)), Multiply(Average(expr), Average(expr))))

    // The list of summary statistics to compute, in the form of expressions.
    val statistics = List[(String, Expression => Expression)](
      "count" -> Count,
      "mean" -> Average,
      "stddev" -> stddevExpr,
      "min" -> Min,
      "max" -> Max)

    val outputCols = (if (cols.isEmpty) numericColumns.map(_.prettyString) else cols).toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      row.grouped(outputCols.size).toSeq.zip(statistics).map {
        case (aggregation, (statistic, _)) => Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      statistics.map { case (name, _) => Row(name) }
    }

    // All columns are string type
    val schema = StructType(
      StructField("summary", StringType) :: outputCols.map(StructField(_, StringType))).toAttributes
    LocalRelation(schema, ret)
  }

  /**
   * Returns the first `n` rows.
   * @group action
   * @since 1.3.0
   */
  def head(n: Int): Array[Row] = limit(n).collect()

  /**
   * Returns the first row.
   * @group action
   * @since 1.3.0
   */
  def head(): Row = head(1).head

  /**
   * Returns the first row. Alias for head().
   * @group action
   * @since 1.3.0
   */
  override def first(): Row = head()

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
   * @group rdd
   * @since 1.3.0
   */
  override def map[R: ClassTag](f: Row => R): RDD[R] = rdd.map(f)

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
   * @group rdd
   * @since 1.3.0
   */
  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
   * @group rdd
   * @since 1.3.0
   */
  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  /**
   * Applies a function `f` to all rows.
   * @group rdd
   * @since 1.3.0
   */
  override def foreach(f: Row => Unit): Unit = rdd.foreach(f)

  /**
   * Applies a function f to each partition of this [[DataFrame]].
   * @group rdd
   * @since 1.3.0
   */
  override def foreachPartition(f: Iterator[Row] => Unit): Unit = rdd.foreachPartition(f)

  /**
   * Returns the first `n` rows in the [[DataFrame]].
   * @group action
   * @since 1.3.0
   */
  override def take(n: Int): Array[Row] = head(n)

  /**
   * Returns an array that contains all of [[Row]]s in this [[DataFrame]].
   * @group action
   * @since 1.3.0
   */
  override def collect(): Array[Row] = queryExecution.executedPlan.executeCollect()

  /**
   * Returns a Java list that contains all of [[Row]]s in this [[DataFrame]].
   * @group action
   * @since 1.3.0
   */
  override def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(rdd.collect() : _*)

  /**
   * Returns the number of rows in the [[DataFrame]].
   * @group action
   * @since 1.3.0
   */
  override def count(): Long = groupBy().count().collect().head.getLong(0)

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   * @group rdd
   * @since 1.3.0
   */
  override def repartition(numPartitions: Int): DataFrame = {
    Repartition(numPartitions, shuffle = true, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
   * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
   * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
   * the 100 new partitions will claim 10 of the current partitions.
   * @group rdd
   * @since 1.4.0
   */
  override def coalesce(numPartitions: Int): DataFrame = {
    Repartition(numPartitions, shuffle = false, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
   * This is an alias for `dropDuplicates`.
   * @group dfops
   * @since 1.3.0
   */
  override def distinct: DataFrame = Distinct(logicalPlan)

  /**
   * @group basic
   * @since 1.3.0
   */
  override def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  override def cache(): this.type = persist()

  /**
   * @group basic
   * @since 1.3.0
   */
  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  override def unpersist(): this.type = unpersist(blocking = false)

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Represents the content of the [[DataFrame]] as an [[RDD]] of [[Row]]s. Note that the RDD is
   * memoized. Once called, it won't change even if you change any query planning related Spark SQL
   * configurations (e.g. `spark.sql.shuffle.partitions`).
   * @group rdd
   * @since 1.3.0
   */
  lazy val rdd: RDD[Row] = {
    // use a local variable to make sure the map closure doesn't capture the whole DataFrame
    val schema = this.schema
    queryExecution.executedPlan.execute().mapPartitions { rows =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      rows.map(converter(_).asInstanceOf[Row])
    }
  }

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   * @since 1.3.0
   */
  def toJavaRDD: JavaRDD[Row] = rdd.toJavaRDD()

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
   * @group rdd
   * @since 1.3.0
   */
  def javaRDD: JavaRDD[Row] = toJavaRDD

  /**
   * Registers this [[DataFrame]] as a temporary table using the given name.  The lifetime of this
   * temporary table is tied to the [[SQLContext]] that was used to create this DataFrame.
   *
   * @group basic
   * @since 1.3.0
   */
  def registerTempTable(tableName: String): Unit = {
    sqlContext.registerDataFrameAsTable(this, tableName)
  }

  /**
   * :: Experimental ::
   * Interface for saving the content of the [[DataFrame]] out into external storage.
   *
   * @group output
   * @since 1.4.0
   */
  @Experimental
  def write: DataFrameWriter = new DataFrameWriter(this)

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
   * @group rdd
   * @since 1.3.0
   */
  def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          JacksonGenerator(rowSchema, gen)(iter.next())
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }

          json
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val jrdd = rdd.map(EvaluatePython.rowToArray(_, fieldTypes)).toJavaRDD()
    SerDeUtil.javaToPython(jrdd)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // Deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

  /**
   * @deprecated As of 1.3.0, replaced by `toDF()`.
   */
  @deprecated("use toDF", "1.3.0")
  def toSchemaRDD: DataFrame = this

  /**
   * Save this [[DataFrame]] to a JDBC database at `url` under the table name `table`.
   * This will run a `CREATE TABLE` and a bunch of `INSERT INTO` statements.
   * If you pass `true` for `allowExisting`, it will drop any table with the
   * given name; if you pass `false`, it will throw if the table already
   * exists.
   * @group output
   * @deprecated As of 1.340, replaced by `write().jdbc()`.
   */
  @deprecated("Use write.jdbc()", "1.4.0")
  def createJDBCTable(url: String, table: String, allowExisting: Boolean): Unit = {
    val w = if (allowExisting) write.mode(SaveMode.Overwrite) else write
    w.jdbc(url, table, new Properties)
  }

  /**
   * Save this [[DataFrame]] to a JDBC database at `url` under the table name `table`.
   * Assumes the table already exists and has a compatible schema.  If you
   * pass `true` for `overwrite`, it will `TRUNCATE` the table before
   * performing the `INSERT`s.
   *
   * The table must already exist on the database.  It must have a schema
   * that is compatible with the schema of this RDD; inserting the rows of
   * the RDD in order via the simple statement
   * `INSERT INTO table VALUES (?, ?, ..., ?)` should not fail.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().jdbc()`.
   */
  @deprecated("Use write.jdbc()", "1.4.0")
  def insertIntoJDBC(url: String, table: String, overwrite: Boolean): Unit = {
    val w = if (overwrite) write.mode(SaveMode.Overwrite) else write
    w.jdbc(url, table, new Properties)
  }

  /**
   * Saves the contents of this [[DataFrame]] as a parquet file, preserving the schema.
   * Files that are written out using this method can be read back in as a [[DataFrame]]
   * using the `parquetFile` function in [[SQLContext]].
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().parquet()`.
   */
  @deprecated("Use write.parquet(path)", "1.4.0")
  def saveAsParquetFile(path: String): Unit = {
    if (sqlContext.conf.parquetUseDataSourceApi) {
      write.format("parquet").mode(SaveMode.ErrorIfExists).save(path)
    } else {
      sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
    }
  }

  /**
   * Creates a table from the the contents of this DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   * This will fail if the table already exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().saveAsTable(tableName)`.
   */
  @deprecated("Use write.saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String): Unit = {
    write.mode(SaveMode.ErrorIfExists).saveAsTable(tableName)
  }

  /**
   * Creates a table from the the contents of this DataFrame, using the default data source
   * configured by spark.sql.sources.default and [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(mode).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, mode: SaveMode): Unit = {
    write.mode(mode).saveAsTable(tableName)
  }

  /**
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source and a set of options,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, source: String): Unit = {
    write.format(source).saveAsTable(tableName)
  }

  /**
   * :: Experimental ::
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, source: String, mode: SaveMode): Unit = {
    write.format(source).mode(mode).saveAsTable(tableName)
  }

  /**
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).saveAsTable(tableName)",
    "1.4.0")
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).saveAsTable(tableName)
  }

  /**
   * (Scala-specific)
   * Creates a table from the the contents of this DataFrame based on a given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * Also note that while this function can persist the table metadata into Hive's metastore,
   * the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).saveAsTable(tableName)",
    "1.4.0")
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).saveAsTable(tableName)
  }

  /**
   * Saves the contents of this DataFrame to the given path,
   * using the default data source configured by spark.sql.sources.default and
   * [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().save(path)`.
   */
  @deprecated("Use write.save(path)", "1.4.0")
  def save(path: String): Unit = {
    write.save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path and [[SaveMode]] specified by mode,
   * using the default data source configured by spark.sql.sources.default.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).save(path)`.
   */
  @deprecated("Use write.mode(mode).save(path)", "1.4.0")
  def save(path: String, mode: SaveMode): Unit = {
    write.mode(mode).save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path based on the given data source,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).save(path)`.
   */
  @deprecated("Use write.format(source).save(path)", "1.4.0")
  def save(path: String, source: String): Unit = {
    write.format(source).save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path based on the given data source and
   * [[SaveMode]] specified by mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).mode(mode).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).save(path)", "1.4.0")
  def save(path: String, source: String, mode: SaveMode): Unit = {
    write.format(source).mode(mode).save(path)
  }

  /**
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).save()", "1.4.0")
  def save(
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).save()
  }

  /**
   * (Scala-specific)
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).save()", "1.4.0")
  def save(
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).save()
  }


  /**
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().mode(SaveMode.Append|SaveMode.Overwrite).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(SaveMode.Append|SaveMode.Overwrite).saveAsTable(tableName)", "1.4.0")
  def insertInto(tableName: String, overwrite: Boolean): Unit = {
    write.mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append).insertInto(tableName)
  }

  /**
   * Adds the rows from this RDD to the specified table.
   * Throws an exception if the table already exists.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().mode(SaveMode.Append).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(SaveMode.Append).saveAsTable(tableName)", "1.4.0")
  def insertInto(tableName: String): Unit = {
    write.mode(SaveMode.Append).insertInto(tableName)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // End of deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

}
