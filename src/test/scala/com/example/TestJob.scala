package com.example

import java.util.Properties

import com.sysgears.DataFrames
import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait UsersDao {
  def getAll: DataFrame
}
class SparkUsersDao @Inject()(spark: SparkSession) extends UsersDao {
  override def getAll: DataFrame = spark.read
    .jdbc("/database", "users", new Properties())
}
class TestUsersDao(spark: SparkSession) extends UsersDao {
  override def getAll: DataFrame = DataFrames.threadLocal.read("jdbc", "users")
}


trait AdultsDao {
  def save(dataFrame: DataFrame)
}
class SparkAdultsDao @Inject()(spark: SparkSession) extends AdultsDao {
  override def save(dataFrame: DataFrame): Unit = dataFrame.write
    .jdbc("/database", "adults", new Properties())
}
class TestAdultsDao(spark: SparkSession) extends AdultsDao {
  override def save(dataFrame: DataFrame): Unit = DataFrames.threadLocal.write("jdbc", "adults", dataFrame)
}


class TestJob @Inject()(spark: SparkSession,
                        usersDao: UsersDao,
                        adultsDao: AdultsDao) {
  def run(): Unit = {
    val users = usersDao.getAll

    val adults = users
      .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
      .filter(col("age") >= lit(18))
      .drop(col("first_name"))
      .drop(col("last_name"))

    adultsDao.save(adults)
  }
}
