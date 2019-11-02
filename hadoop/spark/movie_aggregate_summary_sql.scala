
val movieDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/hadoop/raw/movielens/latest/movies")
val ratingDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/hadoop/raw/movielens/latest/ratings")
 
// define your data structures are tables in this context
movieDF.createOrReplaceTempView("movies")
ratingDF.createOrReplaceTempView("ratings")
 
//define a query based on the new dataset
val query = """select m.movieId, m.title, 
				count(1) as rating_count, sum(r.rating) as total_rating, 
				avg(r.rating) as avg_rating, 
				stddev_pop(r.rating) as std_dev, 
				min(r.timestamp) earliest_ts, max(r.timestamp) latest_ts 
			FROM movies m LEFT JOIN ratings r on m.movieId = r.movieId
			GROUP BY m.movieId, m.title"""

//execute the query
val summaryRatingsDF = spark.sql(query).cache

//show the result to the user via the terminal
summaryRatingsDF.show(30, false)

//save to any format
summaryRatingsDF.write.csv("/user/hadoop/output/movie_rating_summary_spark_sql")
