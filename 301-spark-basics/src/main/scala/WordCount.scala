
import org.apache.spark.sql.SparkSession

/**
 * Count the number of occurrences of each word
 * Result: (sopra, 1), (la, 4), …
 */
object WordCount extends App {
  override def main(args: Array[String]): Unit = {

    val username = "mcavallucci"

    val spark = SparkSession.builder.appName("WordCount Spark 2.1").getOrCreate()

    // Setup default parameters
    val dataset = if (args.length >= 1) args(0) else "divinacommedia"
    val nPartitions = if (args.length >= 2) args(1).toInt else 12

    // Create an RDD from the files in the given folder
    val rddDivina = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)
    //Split each word in file
    val rddDivinaSplit = rddDivina.flatMap(x => x.split(" "))
    //Create Key-Value RDD (K=word, V=occurance)
    val rddDivinaKv = rddDivinaSplit.map(x => (x, 1))
    // Compute the count for each word
    val rddWordCount = rddDivinaKv.reduceByKey((x, y) => x + y)

    // Sort by value??
    //sortByKey() -> sort only the key (word)
    //First invert key with value
    val rddSort = rddWordCount.map({case (k,v)=> (v,k)}).sortByKey()
    //Save result to file
    rddSort.saveAsTextFile("hdfs:/user/" + username + "/spark/WordCount")

  }
}
