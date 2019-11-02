unzip ../datasets/movielens/movielens.zip

hdfs dfs -mkdir -p /user/cloudera/raw/movielens/ml-1m/movies
hdfs dfs -mkdir -p /user/cloudera/raw/movielens/ml-1m/ratings

hdfs dfs -put movies.csv /user/cloudera/raw/movielens/ml-1m/movies/ 
hdfs dfs -put ratings.csv /user/cloudera/raw/movielens/ml-1m/ratings/


	
