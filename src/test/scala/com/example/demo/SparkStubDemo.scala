package com.example.demo

import com.sysgears.DataFrameBuilder._
import com.example._
import com.sysgears.{DataFrames, SparkStub}
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

import scala.language.postfixOps

class SparkStubDemo extends AnyFreeSpec {
  "Manual creation of spark session with test DAO" in {
    implicit val spark: SparkSession = SparkStub.create()

    spark.sparkContext.setLogLevel("ERROR")

    val users =
      ! "first_name" | "last_name" | "age" |
      ! "John"       | "Johnson"   | 17    |
      ! "Henry"      | "Petrovich" | 18    |
      ! "Harry"      | "Harrison"  | 19    |

    DataFrames.threadLocal.addReadableTable("jdbc", "users", users)

    new TestJob(spark, new SparkUsersDao(spark), new SparkAdultsDao(spark)).run()

    DataFrames.threadLocal.getWrittenTable("jdbc", "adults").show()
  }
}
