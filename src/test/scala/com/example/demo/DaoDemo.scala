package com.example.demo

import com.sysgears.DataFrameBuilder._
import com.example._
import com.sysgears.DataFrames
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

import scala.language.postfixOps

class DaoDemo extends AnyFreeSpec {
  "Manual creation of spark session with test DAO" in {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val users =
        ! "first_name" | "last_name" | "age" |
        ! "John"       | "Johnson"   | 17    |
        ! "Henry"      | "Petrovich" | 18    |
        ! "Harry"      | "Harrison"  | 19    |

    DataFrames.threadLocal.addReadableTable("jdbc", "users", users)

    new TestJob(spark, new TestUsersDao(spark), new TestAdultsDao(spark)).run()

    DataFrames.threadLocal.getWrittenTable("jdbc", "adults").show()
  }
}
