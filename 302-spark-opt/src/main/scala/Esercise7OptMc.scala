import org.apache.spark.{SparkContext}

object Esercise7OptMc extends App {
  /**
   * Consider the following job on the movielens dataset.
   *
   * In input, it takes:
   * - a CSV file of movies (each row is movie with a movieId, a title, and list of genres)
   * - a CSV file of ratings (each row is a rating [0-5] made on a movieId in a certain year)
   * - a CSV file of tags (each row is a tag associate to a movieId in a certain year)
   *
   * The goal of the job:
   * - return, for each year, the top N movies based on the average rating
   *   - for each movie, the result should list the movie's title and the total number of associated tags
   *   - the result must be ordered by year
   *
   * The procedure:
   * - initialize RDDS from CSV files
   * - count the number of tags for each movie
   * - join movies with the other RDDs to associate the former with the respective ratings and number of tags
   * - aggregate the result to compute the average rating for each movie
   * - group the result by year
   *
   * The goal of the exercise:
   * - think of the optimizations that can be carried out on this job
   * - try implementing them and see how much time/computation you are able to save
   * - do NOT modify anything outside of the core part, nor in the MovieLensParser class
   * - ensure that Spark recomputes everything by re-initializing every RDD
   *
   * IMPORTANT: for a fair comparison, run the spark2-shell or the spark2-submit with these parameters
   * - --num-executors 2
   * - --executor-cores 3
   *
   * @param sc
   */
  def exercise7(sc: SparkContext): Unit = {
    val inputMoviesPath = "/bigdata/dataset/movielens/movies.csv"
    val inputRatingsPath = "/bigdata/dataset/movielens/ratings.csv"
    val inputTagsPath = "/bigdata/dataset/movielens/tags.csv"
    val outputPath = "mc-movielens-output-opt"
    val topN = 10

    /* CORE PART (start) */

    // Initialize RDDs from CSV files
    val rddMovies = sc.textFile(inputMoviesPath).flatMap(MovieLensParser.parseMovieLine)
    val rddRatings = sc.textFile(inputRatingsPath).flatMap(MovieLensParser.parseRatingLine)
    val rddTags = sc.textFile(inputTagsPath).flatMap(MovieLensParser.parseTagLine)


    // rddRatingsKV ((movieId,year),rating) -
    val rddRatingsKV = rddRatings
      .map(r => ((r._1, r._2), r._3))

    // rddRatingPerMovie ( (movieId,year), avgRating ) - Calculate average rating by movie -
    val rddRatingPerMovie = rddRatingsKV
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))

    // rddRatingPerMovie ( movieId, (year,avgRating) ) -
    val rddRatingPerMovieByYear = rddRatingPerMovie
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })

    // rddTagsPerMovie (movieId, nTags) Count tags by movie
    val rddTagsPerMovie = rddTags
      .map(t => (t._1, 1))
      .reduceByKey(_ + _)


    // rddJoinTagRank (movieId,(year,nTags, avgRating) -
    val rddJoinTagRank = rddTagsPerMovie
      .join(rddRatingPerMovieByYear)

    // rddMoviesKV (movieId, title)
    val rddMoviesKV = rddMovies
      .map(m => (m._1, m._2))
    // Create broadcast variable bRddM (movieId, title) -
    val bRddM = sc.broadcast(rddMoviesKV.collectAsMap())


    // join rddJ with rddJoinTagRank (movieId,(title,year,nTag,avgRating)) -
    val rddRatingPerMovieByYear = ???
      .join(rddJoinTagRank)

    // TODO
    /* HOW TO JOIN BROADCAST VARIABLE WITH RDD */

    // rddRatingPerMovie ( year, (title,nTags,avgRating) )
    val rddRatingPerMovieByYear = rddRatingPerMovie
      .map({ case (k, v) => (k._3, (k._2, k._4, k._5)) })


    /* CORE PART (end) */

    // rddRatingPerMovie ( year, (title,nTags,avgRating) ) - Group by year and format the final result
    val result = rddRatingPerMovieByYear
      .groupByKey
      .mapValues(_.toList.sortBy(-_._3).take(topN))
      .coalesce(1)
      .sortByKey()
      .saveAsTextFile(outputPath)
  }

}
