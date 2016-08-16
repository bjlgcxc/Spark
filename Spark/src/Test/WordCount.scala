package Test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
    
    def main(args: Array[String]) {
        
        val conf = new SparkConf().setAppName("WordCount").setMaster("local");
        val sc = new SparkContext(conf);
        
        val line = sc.textFile("./target/input/1.txt");
        line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println);
    
        sc.stop();
    
    }
    
}