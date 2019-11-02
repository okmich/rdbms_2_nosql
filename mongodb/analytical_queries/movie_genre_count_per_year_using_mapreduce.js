//fetch a frequency of genre of movies per year for all years

var mapFn = function(){
	var genres = this.genres;
	var year = this.release_year;
	//emit each genre as count 1
	for (var i = 0; i < genres.length; i++)
		emit({'year': year, 'genre': genres[i]}, 1);
}

var reduceFn = function(key, values) {
	return Array.sum(values);
}

db.movies.mapReduce(mapFn, reduceFn, {out: {inline: 1}, query: {}}) //outputs to screen

db.movies.mapReduce(mapFn, reduceFn, {out: 'genre_yearly_hist', query: {}}) //outputs to a collection

//we will now query the genre_yearly_hist table
//get the frequency distribution of genres in a particular year. Order by frequency in descending order
db.genre_yearly_hist.find({'_id.year' : 1999}, {}).sort({'value': -1})

//get the number of movies for a particular genre through the years
db.genre_yearly_hist.find({'_id.genre' : 'Action'}, {}).sort({'_id.year': -1})

// sql equivalent
//select genre, release_year as year, count(1) from v_movie_genre group by genre, release_year;
