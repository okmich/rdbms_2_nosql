//fetch a histogram of genre of movies per year for all years
db.movies.aggregate([
	{$project : {'release_year' : 1, 'genres' : 1, '_id' : 0}},
	{$unwind : '$genres'},
	{$group : {
		_id : { 'year' : '$release_year', 'genre' : '$genres'},
		value : {$sum : 1}
	}},
	// {$match : {'_id.year' : 2000}}
])

// sql equivalent
//select genre, release_year as year, count(1) from v_movie_genre group by genre, release_year;