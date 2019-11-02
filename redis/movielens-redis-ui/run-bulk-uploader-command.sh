java -cp target/movielens-redis-ui-1.0-SNAPSHOT.jar com.okmich.rdbmstonosql.redis.upload.BulkUploader -file=~/rdbms_2_nosql/dasets/movielens/movielens/users.csv -type=user -truncate=true


java -cp target/movielens-redis-ui-1.0-SNAPSHOT.jar com.okmich.rdbmstonosql.redis.upload.BulkUploader -file=~/rdbms_2_nosql/datasets/movielens/movielens/movies.csv -type=movie -truncate=true


java -cp target/movielens-redis-ui-1.0-SNAPSHOT.jar com.okmich.rdbmstonosql.redis.upload.BulkUploader -file=~/rdbms_2_nosql/datasets/movielens/movielens/ratings.csv -type=rating -truncate=true

##when using a remote redsi remember to add the arguments -host= and -port=

java -jar target/movielens-redis-ui-1.0-SNAPSHOT.jar 