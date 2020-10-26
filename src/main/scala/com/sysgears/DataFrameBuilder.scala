package com.sysgears

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class RowsBuilder(private val values: mutable.MutableList[mutable.MutableList[Any]]) {
  def |(value: Any): RowsBuilder = {
    values.last += value
    this
  }
  def |(rowsBuilder: RowsBuilder): RowsBuilder = {
    values ++= rowsBuilder.values
    this
  }
  def |(implicit spark: SparkSession): DataFrame = {
    val header = values.get(0).orNull
    val firstRow = values
      .get(1)
      .getOrElse(header)
      .zip(header)

    val schema = StructType(firstRow.map { case (value, name) =>
      StructField(name.toString, value match {
        case _: Boolean => BooleanType
        case _: Byte => ByteType
        case _: Short => ShortType
        case _: Int => IntegerType
        case _: Float => FloatType
        case _: Double => DoubleType
        case _: String => StringType
        case _: Date => DateType
        case _: Timestamp => TimestampType
        case _: Array[Byte] => BinaryType
      })
    })

    spark.createDataFrame(values.drop(1).map { Row.fromSeq }, schema)
  }
}

object DataFrameBuilder {
  implicit class RowValue(value: Any) {
    def unary_! : RowsBuilder = RowsBuilder(mutable.MutableList(mutable.MutableList(value)))
  }
}