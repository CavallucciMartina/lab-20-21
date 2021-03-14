import org.apache.spark.sql.SparkSession

/**
 * Return the inverted index of words
 * Result: (sopra, (0)), (la, (0, 1)), …
 */
object InvertedIndex extends App {
  override def main(args: Array[String]): Unit = {

    val username = "mcavallucci"

    val spark = SparkSession.builder.appName("InvertedIndex Spark 2.1").getOrCreate()

    // Setup default parameters
    val dataset = if (args.length >= 1) args(0) else "divinacommedia"
    val nPartitions = if (args.length >= 2) args(1).toInt else 12

    // Create an RDD from the files in the given folder
    val rddDivina = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)
    //Split each word in file
    val rddDivinaSplit = rddDivina.flatMap(x => x.split(" ")).filter(x => x.nonEmpty)
    //Create Key-Value RDD (K=word, V=offset)
    val rddDivinaKv = rddDivinaSplit.zipWithIndex()
    // Group by the key= the word
    val rddWordCount = rddDivinaKv.groupByKey()

    //Save result to file
    rddWordCount.saveAsTextFile("hdfs:/user/" + username + "/spark/InvertedIndex")

  }
}
