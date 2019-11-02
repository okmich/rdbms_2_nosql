//insert a new movie
db.movies.insertOne({
    "title" : "Avenger Endgame (2019)",
    "release_year" : 2019,
    "genres" : [
            "Fantasy",
            "Sci-Fi"
    ]
});

// insert many movies at the same time
db.movies.insertMany([
{"title" : "Glass", "release_year" : 2019, "genres" : [ "Drama", "Sci-Fi", "Thriller" ] },
{"title" : "Fighting with My Family", "release_year" : 2019, "genres" : [ "Biography", "Comedy", "Drama" ] },
{"title" : "Black Panther", "release_year" : 2018, "genres" : [ "Action", "Adventure", "Sci-Fi" ] }])

//search
db.movies.findOne()
//find functions generally take two documents
//the first part is the query predicate definition, 
//the second part controls the projection
db.movies.find({'title' : "Black Panther"}, {}) //.pretty()
//now project only the id and title
db.movies.find({'title' : "Black Panther"}, {'title' : 1, '_id' : 0})

//examples of operations
//search for the 10 Crime and Action movies
db.movies.find({$and : [{'genres' : 'Crime'}, {'genres' : 'Action'}]}).limit(10)
//and some pagnation
db.movies.find({$and : [{'genres' : 'Crime'}, {'genres' : 'Action'}]}).skip(2).limit(10)

//update 
//has to parts: query part and the updating document part
db.movies.update({'title' : "Black Panther"}, {$set : {'release_year' : 2019}})

//delete
db.movies.delete({})


//get the max id
db.movie.find().sort({"_id": -1}).limit(1)