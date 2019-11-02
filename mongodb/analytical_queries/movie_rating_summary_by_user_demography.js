//fetch the summary ratings for all movies by demographics of viewers
db.ratings.aggregate([
	{$lookup : {
	        from : 'movies',
	        localField: 'movie_id',
	        foreignField: '_id',
	        as: 'movie'
	    }
	},
	{$unwind : '$movie'},
	{$project : {'rating' : 1, 'movie.title' : 1, 'movie_id' : 1, 'user_id' : 1, '_id': 0}},
	{$lookup : {
	        from : 'users',
	        localField: 'user_id',
	        foreignField: '_id',
	        as: 'user'
	    }
	},
	{$unwind : '$user'},
	{$project : {'rating' : 1, 'movie.title' : 1, 'movie_id' : 1, 'user_id' : 1, 'user.age_group': 1, 'user.occupation': 1}},
        {$group: {
                '_id' : {'age' : 'movie_id'},
                'k' : {$sum : 1}
            }
        }
])



//select age_group, occupation, count(1) no_movies, sum(rating) total_ratings, 
//avg(rating) average_rating, variance(rating) from v_rating 
//group by age_group, occupation;
