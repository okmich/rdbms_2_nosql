//prepare movie rdd
val movieRDD = sc.textFile("/user/hadoop/raw/movielens/latest/movies").
	filter(!_.startsWith("movieId")).
	map((s:String) => {
		val parts = s.split(",")
		val title = if (parts.length == 3) parts(1)
		else {
			val tss = for (i <- (1 to parts.length - 2)) yield parts(i).replace("\"", "")
			tss mkString ", "
		}
		(parts(0).toInt, title)
	})

val initFn = (tuple: (Float, Long)) => (1L, tuple._1, tuple._2, tuple._2)

val combineFn = (agg: (Long, Float, Long, Long), tuple: (Float, Long)) => {
		(agg._1 + 1, agg._2 + tuple._1, Math.min(agg._3, tuple._2), Math.max(agg._4, tuple._2))
	}

val reduceFn = (agg1: (Long, Float, Long, Long), agg2: (Long, Float, Long, Long)) => {
		(agg1._1 + agg2._1, agg1._2 + agg2._2, Math.min(agg1._3, agg2._3), Math.max(agg1._4, agg2._4))
	}


val ratingRDD = sc.textFile("/user/hadoop/raw/movielens/latest/ratings").
	filter(!_.startsWith("userId")).
	map((l: String) => {
		val parts = l.split(",")
		(parts(1).toInt, (parts(2).toFloat, parts(3).toLong))
	}).
	combineByKey(initFn, combineFn, reduceFn)

//join movie and rating and prepare the result
val finalRDD = movieRDD.leftOuterJoin(ratingRDD).
	map((t:(Int, (String, Option[(Long, Float, Long, Long)]))) => t._2._2 match {
		case None => s"""${t._1} | ${t._2._1} |  |  |  |  """
		case Some(x) => s"""${t._1} | ${t._2._1} | ${x._1} | ${x._2} | ${x._2 / x._1}  | ${x._3} | ${x._3}"""
	})

//save the 
finalRDD.saveAsTextFile("/user/hadoop/output/movielens/spark/movie_rating_summary_with_title")