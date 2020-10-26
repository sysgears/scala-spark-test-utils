package com.sysgears

import java.util.function.Supplier

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class DataFrames() {
  private case class Source(format: String, pathOrTable: String)
  private type DataFramesMap = mutable.HashMap[Source, DataFrame]

  private val readDataFrames: DataFramesMap = new DataFramesMap()
  private val writeDataFrames: DataFramesMap = new DataFramesMap()

  def addReadableTable(format: String, pathOrTable: String, dataFrame: DataFrame): Unit = {
    readDataFrames.put(Source(format, pathOrTable), dataFrame)
  }
  def read(format: String, pathOrTable: String): DataFrame = {
    readDataFrames.getOrElse(
      Source(format, pathOrTable),
      throw new IllegalStateException(s"Can't find file or table '$pathOrTable' for format '$format'")
    )
  }
  def write(format: String, pathOrTable: String, dataFrame: DataFrame): Unit = {
    writeDataFrames.put(Source(format, pathOrTable), dataFrame)
  }
  def getWrittenTable(format: String, pathOrTable: String): DataFrame = {
    writeDataFrames.getOrElse(
      Source(format, pathOrTable),
      throw new IllegalStateException(s"Can't find file or table '$pathOrTable' for format '$format'")
    )
  }
  def reset(): Unit = {
    readDataFrames.clear()
    writeDataFrames.clear()
  }
}

object DataFrames {
  val threadLocal: DataFrames = new DataFrames() {
    private val threadLocal = ThreadLocal.withInitial[DataFrames](new Supplier[DataFrames] {
      override def get(): DataFrames = new DataFrames()
    })

    override def addReadableTable(format: String, pathOrTable: String, dataFrame: DataFrame): Unit = {
      threadLocal.get().addReadableTable(format, pathOrTable, dataFrame)
    }


    override def read(format: String, pathOrTable: String): DataFrame = {
      threadLocal.get().read(format, pathOrTable)
    }

    override def write(format: String, pathOrTable: String, dataFrame: DataFrame): Unit = {
      threadLocal.get().write(format, pathOrTable, dataFrame)
    }

    override def getWrittenTable(format: String, pathOrTable: String): DataFrame = {
      threadLocal.get().getWrittenTable(format, pathOrTable)
    }

    override def reset(): Unit = {
      threadLocal.get().reset()
    }
  }
}