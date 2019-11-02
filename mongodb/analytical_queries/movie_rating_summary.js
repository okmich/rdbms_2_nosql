// the denormalized version
db.ratings_v2.aggregate([
    {$project : {'rating' : 1, 'movie.title' : 1, 'movie_id' : 1, '_id' : 0}},
	{$group : {
		'_id' : {'title' : '$movie.title', 'movie_id' : '$movie_id'},
		'no_rating' : {$sum : 1},
		'average_rating' : {$avg : '$rating'},
		'std_dev' :  {$stdDevPop: "$rating" }
		}
	},
	{$project : {'_id.title' : 1, 'no_rating' : 1, 'average_rating' : 1, 'std_dev' : 1, 'var_rating' : { $pow: ['$std_dev', 2 ] }}}
])


//for a movie, get the rating summary - count, sum, mean and variance for all movies (normalized)
db.ratings.aggregate([
	{$lookup : {
	        from : 'movies',
	        localField: 'movie_id',
	        foreignField: '_id',
	        as: 'movie'
	    }
	},
	{$unwind : '$movie'},
    {$project : {'rating' : 1, 'movie.title' : 1, 'movie_id' : 1, '_id' : 0}},
	{$group : {
		'_id' : {'title' : '$movie.title', 'movie_id' : '$movie_id'},
		'no_rating' : {$sum : 1},
		'average_rating' : {$avg : '$rating'},
		'std_dev' :  { $stdDevPop: "$rating" }
		}
	},
	{$project : {'_id.title' : 1, 'no_rating' : 1, 'average_rating' : 1, 'std_dev' : 1, 'var_rating' : { $pow: ['$std_dev', 2 ] }}}
])


// select title, count(1) no_movies, sum(rating) total_ratings, avg(rating) average_rating,
//variance(rating) from v_rating group by title;
