//joining ratings and user table

//get all ratings where the user gave a 5 star review
db.ratings.aggregate([
	{$match : {"rating" : 5}},
	{$lookup : {
	        from : 'users',
	        localField: 'user_id',
	        foreignField: '_id',
	        as: 'user'
	    }
	},
	{$unwind : '$user'}
])
