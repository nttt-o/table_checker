package ru.beeline.table_checker

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("TableCheckSession")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._


  def compareTables(origTableName: String, postfix: String = "_pub"): Unit = {
    val newTableName: String = origTableName + postfix

    val origTable = spark.table(origTableName)
    val newTable = spark.table(newTableName)

    val origFieldsData = origTable.schema.fields.map(f => Seq(f.name, f.dataType.toString)).toSeq
    val newFieldsData = newTable.schema.fields.map(f => Seq(f.name, f.dataType.toString)).toSeq

    val lenghtOfRow = origFieldsData.head.size
    val criteria = Seq("name", "dataType")

    val tempOrigDF = spark.sparkContext.parallelize(origFieldsData).toDF()
    val origDF = tempOrigDF.select((0 until lenghtOfRow)
        .map(i => col("value")(i).alias(criteria(i) + "_old")): _*)

    val tempNewDF = spark.sparkContext.parallelize(newFieldsData).toDF()
    val newDF = tempNewDF.select((0 until lenghtOfRow)
      .map(i => col("value")(i).alias(criteria(i) + "_new")): _*)

    val joinedDF = origDF.
      join(newDF, lower(col("name_old")) === lower(col("name_new")), "fullouter")
      .orderBy($"name_old")

    val upperCols = joinedDF
      .filter($"dataType_old" === $"dataType_new")
      .select($"name_old")
      .collect()
      .map(_.getString(0))

    val notInNewCols = joinedDF
      .filter($"name_new".isNull)
      .select($"name_old")
      .collect()
      .map(_.getString(0))

    val newCols = joinedDF
      .filter($"name_old".isNull)
      .select($"name_new")
      .collect()
      .map(_.getString(0))

    val upperAndType = joinedDF
      .filter(($"name_old" =!= $"name_new") && ($"dataType_old" =!= $"dataType_new"))
      .select($"name_old")
      .collect()
      .map(_.getString(0))

    val sameCols = joinedDF
      .filter(($"name_old" === $"name_new") && ($"dataType_old" === $"dataType_new"))
      .select($"name_old")
      .collect()
      .map(_.getString(0))

    println(f"\n\nСтарое название - $origTableName, новое - $newTableName\n")
    joinedDF.show(joinedDF.count().toInt, false)

    println(f"Ничего не поменялось: ${sameCols.mkString(", ")}\n")
    println(f"Название из строчных в прописные, тот же тип: ${upperCols.mkString(", ")}\n")
    println(f"Название из строчных в прописные, разный тип: ${upperAndType.mkString(", ")}\n")
    println(f"Название есть в старом, нет в новом: ${notInNewCols.mkString(", ")}\n")
    println(f"Названия нет в старом, есть в новом: ${newCols.mkString(", ")}\n")

  }

}
