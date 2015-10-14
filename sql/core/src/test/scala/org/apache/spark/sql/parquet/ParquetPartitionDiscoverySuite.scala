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
package org.apache.spark.sql.parquet

import java.io.File
import java.math.BigInteger
import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sources.PartitioningUtils._
import org.apache.spark.sql.sources.{LogicalRelation, Partition, PartitionSpec}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, QueryTest, Row, SQLContext}

// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)

// The data that also includes the partitioning key
case class ParquetDataWithKey(intField: Int, pi: Int, stringField: String, ps: String)

class ParquetPartitionDiscoverySuite extends QueryTest with ParquetTest {
  override val sqlContext: SQLContext = TestSQLContext

  import sqlContext._
  import sqlContext.implicits._

  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"

  test("column type inference") {
    def check(raw: String, literal: Literal): Unit = {
      assert(inferPartitionColumnValue(raw, defaultPartitionName) === literal)
    }

    check("10", Literal.create(10, IntegerType))
    check("1000000000000000", Literal.create(1000000000000000L, LongType))
    check("1.5", Literal.create(1.5, FloatType))
    check("hello", Literal.create("hello", StringType))
    check(defaultPartitionName, Literal.create(null, NullType))
  }

  test("parse partition") {
    def check(path: String, expected: Option[PartitionValues]): Unit = {
      assert(expected === parsePartition(new Path(path), defaultPartitionName))
    }

    def checkThrows[T <: Throwable: Manifest](path: String, expected: String): Unit = {
      val message = intercept[T] {
        parsePartition(new Path(path), defaultPartitionName).get
      }.getMessage

      assert(message.contains(expected))
    }

    check("file://path/a=10", Some {
      PartitionValues(
        ArrayBuffer("a"),
        ArrayBuffer(Literal.create(10, IntegerType)))
    })

    check("file://path/a=10/b=hello/c=1.5", Some {
      PartitionValues(
        ArrayBuffer("a", "b", "c"),
        ArrayBuffer(
          Literal.create(10, IntegerType),
          Literal.create("hello", StringType),
          Literal.create(1.5, FloatType)))
    })

    check("file://path/a=10/b_hello/c=1.5", Some {
      PartitionValues(
        ArrayBuffer("c"),
        ArrayBuffer(Literal.create(1.5, FloatType)))
    })

    check("file:///", None)
    check("file:///path/_temporary", None)
    check("file:///path/_temporary/c=1.5", None)
    check("file:///path/_temporary/path", None)
    check("file://path/a=10/_temporary/c=1.5", None)
    check("file://path/a=10/c=1.5/_temporary", None)

    checkThrows[AssertionError]("file://path/=10", "Empty partition column name")
    checkThrows[AssertionError]("file://path/a=", "Empty partition column value")
  }

  test("parse partitions") {
    def check(paths: Seq[String], spec: PartitionSpec): Unit = {
      assert(parsePartitions(paths.map(new Path(_)), defaultPartitionName) === spec)
    }

    check(Seq(
      "hdfs://host:9000/path/a=10/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(Partition(Row(10, "hello"), "hdfs://host:9000/path/a=10/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), "hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(10.5, "hello"), "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello",
      "hdfs://host:9000/path/a=10.5/_temporary",
      "hdfs://host:9000/path/a=10.5/_TeMpOrArY",
      "hdfs://host:9000/path/a=10.5/b=hello/_temporary",
      "hdfs://host:9000/path/a=10.5/b=hello/_TEMPORARY",
      "hdfs://host:9000/path/_temporary/path",
      "hdfs://host:9000/path/a=11/_temporary/path",
      "hdfs://host:9000/path/a=10.5/b=world/_temporary/path"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), "hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(10.5, "hello"), "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=20",
      s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), s"hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(null, "hello"), s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=$defaultPartitionName",
      s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, null), s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(Row(10.5, null), s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))

    check(Seq(
      s"hdfs://host:9000/path1",
      s"hdfs://host:9000/path2"),
      PartitionSpec.emptySpec)
  }

  test("read partitioned table - normal case") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        val dir = makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps)
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          dir)
        // Introduce _temporary dir to test the robustness of the schema discovery process.
        new File(dir.toString, "_temporary").mkdir()
      }
      // Introduce _temporary dir to the base dir the robustness of the schema discovery process.
      new File(base.getCanonicalPath, "_temporary").mkdir()

      println("load the partitioned table")
      read.parquet(base.getCanonicalPath).registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table - partition key included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      read.parquet(base.getCanonicalPath).registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, 1, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls") {
    withTempDir { base =>
      for {
        // Must be `Integer` rather than `Int` here. `null.asInstanceOf[Int]` results in a zero...
        pi <- Seq(1, null.asInstanceOf[Integer])
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      val parquetRelation = read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath)
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi IS NULL"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, null, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
          } yield Row(i, i.toString, pi, null))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      val parquetRelation = read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath)
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, null))
      }
    }
  }

  test("read partitioned table - merging compatible schemas") {
    withTempDir { base =>
      makeParquetFile(
        (1 to 10).map(i => Tuple1(i)).toDF("intField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 1))

      makeParquetFile(
        (1 to 10).map(i => (i, i.toString)).toDF("intField", "stringField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 2))

      read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath).registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          (1 to 10).map(i => Row(i, null, 1)) ++ (1 to 10).map(i => Row(i, i.toString, 2)))
      }
    }
  }

  test("SPARK-7749 Non-partitioned table should have empty partition spec") {
    withTempPath { dir =>
      (1 to 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(dir.getCanonicalPath)
      val queryExecution = read.parquet(dir.getCanonicalPath).queryExecution
      queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: ParquetRelation2) =>
          assert(relation.partitionSpec === PartitionSpec.emptySpec)
      }.getOrElse {
        fail(s"Expecting a ParquetRelation2, but got:\n$queryExecution")
      }
    }
  }

  test("SPARK-7847: Dynamic partition directory path escaping and unescaping") {
    withTempPath { dir =>
      val df = Seq("/", "[]", "?").zipWithIndex.map(_.swap).toDF("i", "s")
      df.write.format("parquet").partitionBy("s").save(dir.getCanonicalPath)
      checkAnswer(read.parquet(dir.getCanonicalPath), df.collect())
    }
  }

  test("Various partition value types") {
    val row =
      Row(
        100.toByte,
        40000.toShort,
        Int.MaxValue,
        Long.MaxValue,
        1.5.toFloat,
        4.5,
        new java.math.BigDecimal(new BigInteger("212500"), 5),
        new java.math.BigDecimal(2.125),
        java.sql.Date.valueOf("2015-05-23"),
        new Timestamp(0),
        "This is a string, /[]?=:",
        "This is not a partition column")

    // BooleanType is not supported yet
    val partitionColumnTypes =
      Seq(
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType(10, 5),
        DecimalType.Unlimited,
        DateType,
        TimestampType,
        StringType)

    val partitionColumns = partitionColumnTypes.zipWithIndex.map {
      case (t, index) => StructField(s"p_$index", t)
    }

    val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
    val df = createDataFrame(sparkContext.parallelize(row :: Nil), schema)

    withTempPath { dir =>
      df.write.format("parquet").partitionBy(partitionColumns.map(_.name): _*).save(dir.toString)
      val fields = schema.map(f => Column(f.name).cast(f.dataType))
      checkAnswer(read.load(dir.toString).select(fields: _*), row)
    }
  }

  test("SPARK-8037: Ignores files whose name starts with dot") {
    withTempPath { dir =>
      val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

      df.write
        .format("parquet")
        .partitionBy("b", "c", "d")
        .save(dir.getCanonicalPath)

      Files.touch(new File(s"${dir.getCanonicalPath}/b=1", ".DS_Store"))
      Files.createParentDirs(new File(s"${dir.getCanonicalPath}/b=1/c=1/.foo/bar"))

      checkAnswer(read.format("parquet").load(dir.getCanonicalPath), df)
    }
  }
}
