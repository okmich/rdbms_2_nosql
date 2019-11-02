tar xfz complete.csv.tar.gz

# run scale code to generate the json file in current directory
scala -cp scala-csv_2.12-1.3.5.jar csv_to_json.scala
