//explain
db.users.find().explain()
db.users.find({'age_id' : 1}).explain()
db.users.find({$and : [{'age_id' : 1}, {'gender' : 'F'}]}).limit(10).explain()

//add executionStats to explain output
db.users.find({'age_id' : 1}).explain("executionStats")

//get indexes on the users collection
db.users.getIndexes()

//to understand the impact of index on a query, it is important to use the explain function 
db.ratings.find({'genres' : 'Action'}).explain()

//create indexes on the rating collection
db.users.createIndex({'age_id' : 1})
db.ratings.createIndexes([{'movie_id' : 1}, {'user_id': 1}], {unique: true})  //with options
//sort the ratings in descending order or time
db.ratings.createIndex({'rated_at' : -1})
//assumming a unique index on user c
db.users.createIndex({'ssn' : 1}, {unique: true}) //many other options. Check the documentaion


//create full text indexes on the tags collection
db.tags.createIndex({
		'tag' : 'text',
		'movie.title' : 'text'
	}, 
	{
		'weights' : {
			'tag' : 5,
			'movie.title' : 10
		},
		'name' : 'tag_text_idx'
})

//search using text index
db.tags.find( { $text: { $search: "bitter" } }, {'tag': 1, 'movie.title' : 1} )

//add the score of the relevance to the search
db.tags.find( 
	{ $text: { $search: "awesome romance" }},
   	{ score: { $meta: "textScore" } } 
)

//sort by the score of the relevance to the search
db.tags.find( 
	{ $text: { $search: "awesome romance" }},
   	{ score: { $meta: "textScore" } } 
).sort({score: { $meta: "textScore"}})

//drop movie_id and user_id indexes on the rating collection
db.ratings.dropIndex('idx44')
