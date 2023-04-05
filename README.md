# docker-hive

Описание/Пошаговая инструкция выполнения домашнего задания:
Выбрать любой интересный открытый набор данных для работы (можно воспользоваться сайтом Kaggle, пример набора данных: https://www.kaggle.com/tylerx/flights-and-airports-data)
Создать database в Hive и загрузить туда эти таблицы
На этих данных построить витрины (5-6) с использованием конструкций: where, count, group by, having, order by, join, union, window.

1. Использован хайв в докере, проект https://github.com/big-data-europe/docker-hive
2. Использован датасет Global Terrorism Database, 
   More than 180,000 terrorist attacks worldwide, 1970-2017 https://www.kaggle.com/datasets/START-UMD/gtd
3. Копируем датасет на ноду докера
```
    docker cp globalterrorismdb_0718dist.csv docker-hive-hive-server-1:/opt/hive/examples/files/gtd.csv
```
4. Кол-во столбцов в датасете большое, что бы упростить задачу по созданию таблицы в хайве, используем спарк для генерации DDL
```
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def dataFrameToDDL(dataFrame: DataFrame, tableName: String): String = {
    val columns = dataFrame.schema.map { field =>
      "  " + field.name + " " + field.dataType.simpleString.toUpperCase
    }

    s"CREATE TABLE $tableName (\n${columns.mkString(",\n")}\n)"
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark-csv")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val gtd = spark
                  .read
                  .option("header", value = true)
                  .option("inferSchema", value = true)
                  .csv("/home/vadim/MyExp/docker-hive/globalterrorismdb_0718dist.csv")
    gtd.show()
    println(dataFrameToDDL(gtd,"gtd_table"))
  }
}
```
5. SQL код ДЗ в файле
```
homework.sql
```

