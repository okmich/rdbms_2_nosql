docker pull elasticsearch:7.3.0
docker run -d --name elasticsearch01 -e "discovery.type=single-node" -p 9200:9200 elasticsearch:7.3.0

docker pull kibana:7.3.0
docker run -d --name kibana01 --link elasticsearch01:elasticsearch -p 5601:5601 kibana:7.3.0

docker pull logstash:7.3.0