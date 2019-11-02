val rdd = sc.textFile("/user/hadoop/raw/movielens/latest/movies")
//filtering
val filteredRdd = rdd.filter(!_.startsWith("movieId"))
//projection
val genresRDD = filteredRdd.map((s:String) => {
val parts = s.split(",")
parts(parts.length - 1) 
})
//transformation
val singleRDD = genresRDD.flatMap(_.split("\\|"))
val kvRDD = singleRDD.map((s:String) => (s, 1L))
//aggregation
val reducedRDD = kvRDD.reduceByKey(_ + _)
//similar to kvRDD.reduceByKey((a: Long, b: Long) => a + b)
val resultRDD = reducedRDD.map((t : (String, Long)) => s"${String.format("%-30s", t._1)}|\t${t._2}")

//storing output
resultRDD.saveAsTextFile("/user/hadoop/output/movielens/spark/genre_histogram")

// succinctly
// val rdd = sc.textFile("/user/hadoop/movielens/latest/movies").
// 		filter(!_.startsWith("movieId")).
// 		map((s:String) => {
// 			val parts = s.split(",")
// 			parts(parts.length - 1)
// 		}).
// 		flatMap(_.split("\\|")).
// 		map((s:String) => (s, 1)).
// 		reduceByKey(_ + _).
//		map((t : (String, Long)) => s"${String.format("%-30s", t._1)}|\t${t._2}")

// rdd.saveAsTextFile("/user/hadoop/output/movielens/spark/genre_histogram")		
