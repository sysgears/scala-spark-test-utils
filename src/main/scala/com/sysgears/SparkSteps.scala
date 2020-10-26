package com.sysgears

import io.cucumber.datatable.DataTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import DataSetConverter._
import com.google.inject.Inject
import com.google.inject.name.Named
import io.cucumber.java.Before
import io.cucumber.java.en.{Given, Then}
import org.apache.spark.sql.functions.col

class SparkSteps @Inject()(spark: SparkSession,
                           dataFrames: DataFrames,
                           @Named("cucumber.spark.datatype-packages") dataTypesPackages: Array[String]) {
  private implicit val sparkSession: SparkSession = spark

  @Before
  def setUp(): Unit = {
    dataFrames.reset()
  }

  @Given("spark format: {string} for table: {string} has data with defaults {string}:")
  def setDataFrame(format: String, tableOrPath: String, defaults: String, dataTable: DataTable): Unit = {
    dataFrames.addReadableTable(format, tableOrPath, dataTable.asDataFrame(defaults))
  }

  @Given("spark format: {string} for table: {string} has data:")
  def setDataFrame(format: String, tableOrPath: String, dataTable: DataTable): Unit = {
    dataFrames.addReadableTable(format, tableOrPath, dataTable.asDataFrame())
  }

  @Given("spark format: {string} for table: {string} has data with defaults {string} as {string}:")
  def setTypeCheckedDataFrame(format: String, tableOrPath: String, defaults: String, `class`: String,  dataTable: DataTable): Unit = {
    dataFrames.addReadableTable(format, tableOrPath, dataTable.asTypeCheckedDataFrame(findClass(`class`), defaults))
  }

  @Given("spark format: {string} for table: {string} has data with defaults as {string}:")
  def setTypeCheckedDataFrame(format: String, tableOrPath: String, `class`: String,  dataTable: DataTable): Unit = {
    val foundClass = findClass(`class`)
    val defaults = findNotUsedFields(foundClass, dataTable)
    dataFrames.addReadableTable(format, tableOrPath, dataTable.asTypeCheckedDataFrame(foundClass, defaults))
  }

  @Given("spark format: {string} for table: {string} has data as {string}:")
  def setTypeCheckedDataFrameWithoutDefaults(format: String, tableOrPath: String, `class`: String, dataTable: DataTable): Unit = {
    dataFrames.addReadableTable(format, tableOrPath, dataTable.asTypeCheckedDataFrame(findClass(`class`)))
  }


  @Then("spark format: {string} for table: {string} wrote data with defaults {string}:")
  def assertDataFrame(format: String, tableOrPath: String, defaults: String, dataTable: DataTable): Unit = {
    assertEquals(
      dataTable.asDataFrame(defaults),
      dataFrames.getWrittenTable(format, tableOrPath)
    )
  }

  @Then("spark format: {string} for table: {string} wrote data:")
  def assertDataFrame(format: String, tableOrPath: String, dataTable: DataTable): Unit = {
    assertEquals(
      dataTable.asDataFrame(),
      dataFrames.getWrittenTable(format, tableOrPath)
    )
  }

  @Then("spark format: {string} for table: {string} wrote data with defaults {string} as {string}:")
  def assertTypeCheckedDataFrame(format: String, tableOrPath: String, defaults: String, `class`: String,  dataTable: DataTable): Unit = {
    assertEquals(
      dataTable.asTypeCheckedDataFrame(findClass(`class`), defaults),
      dataFrames.getWrittenTable(format, tableOrPath)
    )
  }

  @Then("spark format: {string} for table: {string} wrote data with defaults as {string}:")
  def assertTypeCheckedDataFrame(format: String, tableOrPath: String, `class`: String,  dataTable: DataTable): Unit = {
    val foundClass = findClass(`class`)
    val defaults = findNotUsedFields(foundClass, dataTable)

    assertEquals(
      dataTable.asTypeCheckedDataFrame(foundClass, defaults),
      dataFrames.getWrittenTable(format, tableOrPath)
    )
  }

  @Then("spark format: {string} for table: {string} wrote data as {string}:")
  def assertTypeCheckedDataFrameWithoutDefaults(format: String, tableOrPath: String, `class`: String, dataTable: DataTable): Unit = {
    assertEquals(
      dataTable.asTypeCheckedDataFrame(findClass(`class`)),
      dataFrames.getWrittenTable(format, tableOrPath)
    )
  }


  def assertEquals(expected: DataFrame, actual: DataFrame): Unit = {
    val expectedWithSortedColumns = expected.select(
      expected.columns.sorted.map(col):_*
    )
    val actualWithSortedColumns = actual.select(
      expected.columns.sorted.map(col):_*
    )

    def printErrorInfo(): Unit = {
      println("Expected")
      expectedWithSortedColumns.show()
      println("Actual")
      actualWithSortedColumns.show()
    }

    val diff = expectedWithSortedColumns.except(actualWithSortedColumns)
    if (diff.count() > 0) {
      printErrorInfo()
      println("Diff")
      diff.show()
      throw new AssertionError("Actual data frame doesn't have expected rows")
    }

    val diff2 = actualWithSortedColumns.except(expectedWithSortedColumns)
    if (diff2.count() > 0) {
      printErrorInfo()
      println("Diff")
      diff2.show()
      throw new AssertionError("Actual data frame has extra rows")
    }
  }


  private def findClass(`class`: String): Class[_] = {
    def loadClass(name: String): Option[Class[_]] = {
      try {
        Some(
          Thread.currentThread()
            .getContextClassLoader
            .loadClass(name)
        )
      } catch {
        case e: ClassNotFoundException => None
      }
    }

    val classNames = dataTypesPackages.map { pack => pack + "." + `class` } :+ `class`
    for (className <- classNames; clazz <- loadClass(className)) {
      return clazz
    }
    throw new ClassNotFoundException("Tried classes: " + classNames.mkString(", "))
  }

  private def findNotUsedFields(`class`: Class[_], dataTable: DataTable): String = {
    val usedFields = dataTable.asLists().get(0)
    val defaults = `class`.getDeclaredFields
      .map { _.getName }
      .filter { field => !usedFields.contains(field) }
      .mkString(", ")
    defaults
  }
}