
//create a capped collection that can contain maximum of 200 document
db.createCollection('clicks', {
    capped : true, 
    size : 2000000, 
    max : 200
});

//load 200 documents into the collection
for (var i = 1; i <= 200; i++){
    db.clicks.save({'field' : 'x', 'counter' : i})
}

//count how many document they are
db.clicks.count({'counter' : {$lt : 200}})

//load another 100 documents into the collection
for (var i = 201; i <= 300; i++){
    db.clicks.save({'field' : 'x', 'counter' : i})
}

//count how many document they are with counter number less than 200
db.clicks.count({'counter' : {$lt : 200}})