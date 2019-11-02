// fetch the summary ratings for all movies by demographics of viewers by genre by yearMonth
db.ratings_v2.aggregate([
    {$project : {'rating' : 1, 'ryear' : {'$year' : '$rated_at'} ,  'rmonth' : {'$month' : '$rated_at'} , 'movie.title' : 1, 'movie_id' : 1, 'user.age_group' : 1, 'user.occupation' : 1, 'movie.genres':1, '_id' : 0}},
    {$unwind : '$movie.genres'},
    {$group : {
                '_id' : {'year' : '$ryear', 'month' : '$rmonth', 'title' : '$movie.title', 'age_group' : '$user.age_group', 'occupation' : '$user.occupation', 'genre' : '$movie.genres'},
                'no_rating' : {$sum : 1},
                'average_rating' : {$avg : '$rating'},
				'var_rating' : { $pow: [ { '$stdDevPop' : '$rating' }, 2 ] } ,
        }
    }
])


// select age_group, occupation, gm.genre, monthname(r.rated_at) monthname, count(1) no_movies from v_rating r
//  join v_movie_genre gm on gm.movie_id = r.movie_id
// group by age_group, occupation, gm.genre, monthname;