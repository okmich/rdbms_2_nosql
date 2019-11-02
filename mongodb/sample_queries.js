//get all movies released in the year 2000
db.movies.find({'release_year': 1995})

//get any ten female users who are teens
db.users.find({$and : [{'age_id' : 1}, {'gender' : 'F'}]}).limit(10)

//get any ten lawyers or female users who are teens 
db.users.find({$or : [
		{'occupation' : 'Lawyer'}, 
		{$and : [{'age_id' : 1}, {'gender' : 'F'}]
	}]
}).limit(10)

//get the count of teenage female users
db.users.count({$and : [{'age_id' : 1}, {'gender' : 'F'}]})
