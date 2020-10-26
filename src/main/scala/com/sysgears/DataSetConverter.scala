package com.sysgears

import java.sql.{Date, Timestamp}

import io.cucumber.datatable.DataTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.language.implicitConversions

object DataSetConverter {
  implicit class DataTableExtension(dataTable: DataTable) {
    def asDataFrame(defaults: String = "")(implicit spark: SparkSession): DataFrame = {
      def parseValue(`type`: String, value: String) = {
        `type` match {
          case "BOOLEAN" => value.toBoolean
          case "BYTE" | "TINYINT" => value.toByte
          case "SHORT" | "SMALLINT" => value.toShort
          case "INT" | "INTEGER" => value.toInt
          case "LONG" | "BIGINT" => value.toLong
          case "FLOAT" | "REAL" => value.toFloat
          case "DATE" => Date.valueOf(value)
          case "TIMESTAMP" => Timestamp.valueOf(value)
          case "BINARY" => value.getBytes
          case "DECIMAL" | "DEC" | "NUMERIC" => BigDecimal(value)
          case _ => value
        }
      }

      def defaultValue(`type`: String) = {
        `type` match {
          case "BOOLEAN" => false
          case "BYTE" | "TINYINT" | "SHORT" | "SMALLINT" | "INT" | "INTEGER" | "LONG" | "BIGINT" | "FLOAT" | "REAL" =>
            parseValue(`type`, "0")
          case "DATE" => new Date(System.currentTimeMillis())
          case "TIMESTAMP" => new Timestamp(System.currentTimeMillis())
          case "BINARY" => "".getBytes
          case "DECIMAL" | "DEC" | "NUMERIC" => BigDecimal(0)
          case _ => ""
        }
      }

      val header = dataTable.asLists().get(0)
      val defaultsHeader = defaults.split(",")
        .filter { !_.isEmpty }
        .map { _.trim }

      val namesAndTypes = header
          .map { nameAndType => nameAndType.split(" ") }
          .map { nameAndTypeSplit => (nameAndTypeSplit(0), nameAndTypeSplit(1)) }

      val defaultValues = defaultsHeader
          .map { nameAndType => nameAndType.split(" ") }
          .map { nameAndTypeSplit => defaultValue(nameAndTypeSplit(1)) }

      spark.createDataFrame(
        dataTable.asMaps()
          .map { rows => rows.map { case (key, value) => (key.split(" ")(0), value) } }
          .map { rows =>
            namesAndTypes
              .map { case (name, typ) => parseValue(typ, rows.getOrDefault(name, "")) }
              .++(defaultValues)
          }
          .map(Row.fromSeq(_)),
        StructType.fromDDL((header ++ defaultsHeader).mkString(", "))
      )
    }

    def asTypeCheckedDataFrame(`class`: Class[_], defaults: String = "")(implicit spark: SparkSession): DataFrame = {
      def addTypeToColumn(fieldName: String) = {
        `class`.getDeclaredField(fieldName).setAccessible(true)
        val `type` = `class`.getDeclaredField(fieldName).getType
        val sparkType = `type` match {
          case t if t == classOf[Boolean] =>  "BOOLEAN"
          case t if t == classOf[Byte] => "BYTE"
          case t if t == classOf[Short] => "SHORT"
          case t if t == classOf[Int] => "INT"
          case t if t == classOf[Long] => "LONG"
          case t if t == classOf[Float] => "FLOAT"
          case t if t == classOf[Date] => "DATE"
          case t if t == classOf[Timestamp] => "TIMESTAMP"
          case t if t == classOf[Array[Byte]] => "BINARY"
          case t if t == classOf[BigDecimal] => "DECIMAL"
          case _ => "STRING"
        }
        s"$fieldName $sparkType"
      }

      val tableWithTypes = DataTable.create(dataTable.asLists()
        .zipWithIndex
        .map { case (row, rowNumber: Int) =>
          if (rowNumber == 0) {
            JavaConversions.seqAsJavaList(row.map(addTypeToColumn))
          } else {
            row
          }
        }
      )

      val defaultsWithTypes = defaults.split(",")
        .filter { !_.isEmpty }
        .map { _.trim() }
        .map { addTypeToColumn }
        .mkString(",")

      tableWithTypes.asDataFrame(defaultsWithTypes)
    }
  }
}
