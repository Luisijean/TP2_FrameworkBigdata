package samples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import samples.config.JsonConfigProtocol.jsonConfig
import samples.config.{ConfigReader, JsonConfig}
import samples.utils.DataFrameSchema
import spray.json._



object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[JsonConfig]
    val dfSchema: StructType = DataFrameSchema.buildDataframeSchema(configuration.fields)

    // Question 1
    val data = DataFrameReader.readCsv("input_data/*.csv", configuration.csvOptions, dfSchema)
    data.printSchema()
    data.show(40,false)

    // Qustion 2
    spark.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    val question2 = data.withColumn("date", callUDF("get_file_name", input_file_name()))
    question2.show(40,false)




    // Question 3


    //val df = question2.withColumn("toDate", to_date(concat(col("date")), "ddMMyyyy"))
      //.show(80, false)

  //val new_df = question2.withColumn("new_date", array(col("toDate"), date_add(col("toDate"), -1)))
    //.drop("toDate")
    //.selectExpr("explode(new_date) as toDate", "*")
    //.drop("new_date")
    //.show(80, false)


  //Question 4

    question2.write.mode(SaveMode.Overwrite).partitionBy("date").csv("output")



  }

}