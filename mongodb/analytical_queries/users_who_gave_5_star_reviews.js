//get all users who gave a 5 star review
db.ratings.aggregate([
	{$match : {"rating" : 5}},
	{$lookup : {
	        from : 'users',
	        localField: 'user_id',
	        foreignField: '_id',
	        as: 'user'
	    }
	},
	{$unwind : '$user'},
	{$project : {'user' : '$user'}}
])

//get all ratings where the user gave a 5 start review
db.ratings_v2.aggregate([
    {$match : {"rating" : 5}},
    //{$project: {'user' : '$user', '_id' : 0}}
    {$project: {'user_id' : '$user.user_id', 'gender' : '$user.gender', 'occupation' : '$user.occupation', 'age_group' : '$user.age_group'}}
])


//or return all users who have given five star ratings and the movies they gave them on
db.ratings.aggregate([
	{$match : {"rating" : 5}},
	{$lookup : {
	        from : 'users',
	        localField: 'user_id',
	        foreignField: '_id',
	        as: 'user'
	    }
	},
	{$unwind : '$user'},
	{$project : {'movie_id' : 1, 'user' : 1, '_id': 0}},
	{$lookup : {
	        from : 'movies',
	        localField: 'movie_id',
	        foreignField: '_id',
	        as: 'movie'
	    }
	},
    {$group : {
            _id : '$user_id',
            'movies' : { $addToSet: { movie: "$movie.title" } }
        }
    }
])


//or return all users who have given five star ratings and the movies they gave them on
db.ratings_v2.aggregate([
	{$match : {"rating" : 5}},
    {$project: {'user' : '$user', 'movie' : 1, '_id': 0}},
    {$group : {
            _id : '$user.user_id',
            'movies' : { $addToSet: { movie: "$movie.title" } }
        }
    }
])
