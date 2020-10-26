package com.sysgears

import java.lang.reflect.Modifier

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, SparkSession}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * This utils is experimental, be careful while using it
 */
object SparkStub {
  type Format = String
  type TableNameOptionKey = String

  def create(dataFrames: DataFrames = DataFrames.threadLocal,
             tableResolver: Format => TableNameOptionKey = _ => null): SparkSession = {
    val session = Mockito.spy(SparkSession.builder()
        .master("local[1]")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    Mockito
      .doReturn(createDataFrameReader(session, dataFrames, tableResolver), Array():_*)
      .when(session)
      .read

    session
  }

  private def createDataFrameReader(sparkSession: SparkSession,
                            dataFrames: DataFrames,
                            tableResolver: Format => TableNameOptionKey) = {
    val testReader = Mockito.spy(sparkSession.read)

    Mockito.doAnswer(new Answer[DataFrame]() {
      override def answer(invocation: InvocationOnMock): DataFrame = {
        val (format, tableOrPath) = getFormatAndTable(testReader, tableResolver, invocation)

        stubDataFrameMethods(dataFrames.read(format, tableOrPath), dataFrames, tableResolver)
      }
    })
      .when(testReader)
      .load()

    testReader
  }

  private def createDataFrameWriter(originalWriter: DataFrameWriter[_],
                                    dataFrames: DataFrames,
                                    tableResolver: Format => TableNameOptionKey) = {
    val testWriter = Mockito.spy(originalWriter)

    Mockito.doAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val (format, tableOrPath) = getFormatAndTable(testWriter, tableResolver, invocation)

        dataFrames.write(format, tableOrPath, getPrivateField(testWriter)("df"))
      }
    })
      .when(testWriter)
      .save()

    testWriter
  }

  private def stubDataFrameMethods(ds: DataFrame,
                                   dataFrames: DataFrames,
                                   tableResolver: Format => TableNameOptionKey): DataFrame = {
    val dfStub = Mockito.spy(ds)
    classOf[DataFrame]
      .getMethods
      .filter { method => method.getReturnType.isAssignableFrom(classOf[Dataset[_]]) }
      .filter { method => !Modifier.isStatic(method.getModifiers) }
      .foreach { method =>
        val doAnswer = Mockito.doAnswer(new Answer[Any]() {
          override def answer(invocation: InvocationOnMock): Dataset[_] = {
            stubDataFrameMethods(invocation.callRealMethod().asInstanceOf[DataFrame], dataFrames, tableResolver)
          }
        })
          .when(dfStub)
        method.invoke(doAnswer, method.getParameterTypes.map { clazz => ArgumentMatchers.any(clazz).asInstanceOf[AnyRef] }:_*)
      }

    val originalWriter = ds.write
    Mockito
        .doAnswer(new Answer[Any]() {
          override def answer(invocation: InvocationOnMock): DataFrameWriter[_] = {
            createDataFrameWriter(originalWriter, dataFrames, tableResolver)
          }
        })
        .when(dfStub)
        .write

    dfStub
  }

  private def getFormatAndTable[T: ClassTag](readerOrWriter: T,
                                tableResolver: Format => TableNameOptionKey,
                                invocation: InvocationOnMock): (String, String) = {
    val format: String = getPrivateField(readerOrWriter)("source")

    val tableOption = format match {
      case "jdbc" => JDBCOptions.JDBC_TABLE_NAME
      case "org.apache.spark.sql.cassandra" => "table"
      case _ => tableResolver(format)
    }

    val tableOrPath = if (tableOption != null) {
      getPrivateField(readerOrWriter)("extraOptions")
        .asInstanceOf[mutable.HashMap[String, String]]
        .get(tableOption)
        .orNull
    } else {
      invocation.getArgument(0).asInstanceOf[String]
    }

    (format, tableOrPath)
  }

  private def getPrivateField[T: ClassTag, V](obj: T)(name: String)(implicit tag: ClassTag[T]): V = {
    val field = tag.runtimeClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(obj).asInstanceOf[V]
  }
}
