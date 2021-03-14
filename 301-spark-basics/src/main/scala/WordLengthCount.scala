import org.apache.spark.sql.SparkSession

/**
 * Count the number of occurrences of words of given lengths
 * Result: (2, 4), (5, 8)
 */
object WordLengthCount extends App {
  override def main(args: Array[String]): Unit = {

    val username = "mcavallucci"

    val spark = SparkSession.builder.appName("WordLengthCount Spark 2.1").getOrCreate()

    // Setup default parameters
    val dataset = if (args.length >= 1) args(0) else "divinacommedia"
    val nPartitions = if (args.length >= 2) args(1).toInt else 12

    // Create an RDD from the files in the given folder
    val rddDivina = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)
    //Split each word in file
    val rddDivinaSplit = rddDivina.flatMap(x => x.split(" "))
    //Create Key-Value RDD (K=length, V=occurance)
    val rddDivinaKv = rddDivinaSplit.map(x => (x.length,1))
    // Compute the count for each length
    val rddWordCount = rddDivinaKv.reduceByKey((x, y) => x + y)
    //Save result to file
    rddWordCount.saveAsTextFile("hdfs:/user/" + username + "/spark/WordLengthCount")

  }
}

