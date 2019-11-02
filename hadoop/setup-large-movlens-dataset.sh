wget http://files.grouplens.org/datasets/movielens/ml-latest.zip

unzip ml-latest.zip

hdfs dfs -mkdir -p /user/hadoop/raw/movielens/latest/movies
hdfs dfs -mkdir -p /user/hadoop/raw/movielens/latest/ratings

hdfs dfs -put ml-latest/movies.csv /user/hadoop/raw/movielens/latest/movies/ 
hdfs dfs -put ml-latest/ratings.csv /user/hadoop/raw/movielens/latest/ratings/




	
