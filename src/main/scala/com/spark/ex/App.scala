package com.spark.ex

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import javax.xml.transform.Source
import java.util.Properties


/**
 * @author ${user.name}
 */
object App {

// 
  
  def main(args: Array[String]) {
    println("Hello World!")

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "App")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.crealytics.spark.excel")
 //     .option("SeriesReport", "A12")
      .option("location", "D:/SparkScala/SeriesReport-20182008.xlsx")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "False")
      .load()
      
   
      val columns = df.rdd.map(col => col.toSeq) 
      val nums = df.rdd.count()
      val numOfYears = nums.toInt
      println(numOfYears)
      for(i <- 0 to numOfYears - 1) {
        val sliceColumns = columns.collect()
        //val yearColumns = sliceColumns(0)
        val yearColumns = sliceColumns(i)
        val yearList = yearColumns.toList
        val stringYearList = yearList.map(_.toString)
        val newYearList = stringYearList.map(_.toDouble)
        val intYear = newYearList(0).toInt
        val newYearList1 = newYearList.drop(1)
        val maxValue = newYearList1.max
        val minValue = newYearList1.min
        val sumValue = newYearList1.sum
        val avgValue =  sumValue / newYearList1.length
        
        val seq = Seq((intYear,maxValue,minValue,sumValue,avgValue))
        val newDF = seq.toDF("Year","Max_Value","Min_Value","Sum_Value","Average_Value")
        newDF.show()
        println()
        println("Year : "+intYear)
        println()
        println("Max value : "+maxValue)
        val index = newYearList1.max
        val properties = new Properties()
        properties.put("driver", "com.mysql.jdbc.Driver")
        properties.put("url", "jdbc:mysql://localhost:3306/mysql")
        properties.put("user", "root");
        properties.put("password", "pass")
        //df.show()
        newDF.write.mode("append").jdbc(properties.getProperty("url"), "YEAR_STATS", properties)
//        val headers = df.columns
//        println("Current Column : "+headers(0))
        println("Min value : "+newYearList1.min)
        val sum = newYearList1.sum
        println("Sum value : "+newYearList1.sum)
        val avg = sum / newYearList1.length
        println("Average value : "+avg)
      }
    
      println("Column name : "+df.schema.fields(1))//.foreach(x => println(x))
//      println(janData)
//      
      println()
//      println(newYearList)
//      df.printSchema()
//      println(newYearList.map(x => x.toDouble))
//      val selectData = df
//        .filter(df("Year") === 2008)
//        .select(df("Jan"),df("Feb"),df("Mar"),df("Apr"),df("May"),df("Jun"),df("Jul"),df("Aug"),df("Sep"),df("Oct"),df("Nov"),df("Dec"))
//      val colSelectData = selectData.collect()
      
      
//      colSelectData.foreach(println)
//      val maxData = listData.max
        
 //     colSelectData.foreach(println)
//      columns.foreach(println)
      
      val doubles = df.rdd.map(row => row.getDouble(1))
      //val everyMonth = df.rdd.map(row => row.toSeq).map(_.asInstanceOf[Int]).map((row => row.foldLeft(0)(_ + _)) 
//      val sumDF = df.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2)
      val janSum = doubles.reduce((sum, value) => sum + value)
      val janAvg = janSum / df.rdd.count()
      println("The count is " + df.rdd.count())
      println(janAvg)

      
      //df.rdd.foreach(row => println(row))

    /*val spark: SparkSession = ???
    val exceldf = spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "true") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs // Optional, default: Either inferred schema, or all columns are Strings
      .load("Worktime.xlsx")*/
    //
    //    val df = sqlContext.read
    //      .option("useHeader", "true")
    //      .csv("D:/SparkScala/SeriesReport.csv")
      sc.stop()

  }

}
