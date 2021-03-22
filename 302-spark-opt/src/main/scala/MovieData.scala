object MovieData {
  def extract(row: String) = {
    def getDouble(str: String): Double = {
      if (str.isEmpty)
        0
      else
        str.toDouble
    }

    val columns = row.split(",").map(_.replaceAll("\"", ""))
    val movieId = getDouble(columns(0))
    MovieData(movieId, columns(1), columns(2))
  }
}

case class MovieData(
                      moviedId: Double,
                      title: String,
                      genres: String
                    )
