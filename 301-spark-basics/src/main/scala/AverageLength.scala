import org.apache.spark.sql.SparkSession

/**
 * Count the average length of words given their first letter (hint: check the example in 301-1)
 * Result: (s, 5), (l, 2), …
 */
object AverageLength extends App {
  override def main(args: Array[String]): Unit = {

    val username = "mcavallucci"

    val spark = SparkSession.builder.appName("AverageLength Spark 2.1").getOrCreate()

    // Setup default parameters
    val dataset = if (args.length >= 1) args(0) else "divinacommedia"
    val nPartitions = if (args.length >= 2) args(1).toInt else 12

    // Create an RDD from the files in the given folder
    val rddDivina = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)
    //Split each word in file
    val rddDivinaSplit = rddDivina.flatMap(x => x.split(" ")).filter(x => x.nonEmpty)
    //Create Key-Value RDD (K=first letter, V=length)
    val rddDivinaKv = rddDivinaSplit.map(x => (x.substring(0, 1), x.length))
    // Compute the average length of words given their first letter
    val rddWordCount = rddDivinaKv
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1),
        (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2)).map({case (k, v) => (k, v._1 / v._2) })
    //For compute the average we create 2 numer: numerator and denominator.
    //Denominator has the counter
    //Numerator as the sum of the length
    //Save result to file
    rddWordCount.saveAsTextFile("hdfs:/user/" + username + "/spark/AverageLength")

  }
}
