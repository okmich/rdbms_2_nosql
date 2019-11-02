java -jar movielens-hbase-1.0-SNAPSHOT.jar -tblname=users -file=/home/maria_dev/ml-1m/users.dat -zkhost=sandbox-hdp.hortonworks.com -zkport=2181

java -jar movielens-hbase-1.0-SNAPSHOT.jar -tblname=movies -file=/home/maria_dev/ml-1m/movies.dat -zkhost=sandbox-hdp.hortonworks.com -zkport=2181

java -jar movielens-hbase-1.0-SNAPSHOT.jar -tblname=ratings -file=/home/maria_dev/ml-1m/ratings.dat -zkhost=sandbox-hdp.hortonworks.com -zkport=2181

java -jar movielens-hbase-1.0-SNAPSHOT.jar -tblname=find_genre_movies -zkhost=sandbox-hdp.hortonworks.com -zkport=2181