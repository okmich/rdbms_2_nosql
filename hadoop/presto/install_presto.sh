# download presto binary
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.226/presto-server-0.226.tar.gz

# download the presto cli executable
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.226/presto-cli-0.226-executable.jar

# if you need the jdbc driver for deployment in app servers or application, uncomment the next line
# wget https://repo1.maven.org/maven2/com/facebook/presto/presto-jdbc/0.226/presto-jdbc-0.226.jar


#untar presto server binary, move to a desired location 
tar xfz presto-server-0.226.tar.gz
# add a data folder inside the newly unpacked folder
mkdir ./presto-server-0.226/data
mkdir -p ./presto-server-0.226/etc/catalog

# add some of the config
touch ./presto-server-0.226/etc/node.properties
echo "node.environment=production" >> ./presto-server-0.226/etc/node.properties
echo "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff" >> ./presto-server-0.226/etc/node.properties
echo "node.data-dir=/opt/presto/data" >> ./presto-server-0.226/etc/node.properties

touch ./presto-server-0.226/etc/jvm.config
echo "-server" >> ./presto-server-0.226/etc/jvm.config
echo "-Xmx8G" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:+UseG1GC" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:G1HeapRegionSize=32M" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:+UseGCOverheadLimit" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:+ExplicitGCInvokesConcurrent" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:+HeapDumpOnOutOfMemoryError" >> ./presto-server-0.226/etc/jvm.config
echo "-XX:+ExitOnOutOfMemoryError" >> ./presto-server-0.226/etc/jvm.config

touch ./presto-server-0.226/etc/config.properties
echo "coordinator=true" >> ./presto-server-0.226/etc/config.properties
echo "node-scheduler.include-coordinator=true" >> ./presto-server-0.226/etc/config.properties
echo "http-server.http.port=7001" >> ./presto-server-0.226/etc/config.properties
echo "query.max-memory=5GB" >> ./presto-server-0.226/etc/config.properties
echo "query.max-memory-per-node=1GB" >> ./presto-server-0.226/etc/config.properties
echo "query.max-total-memory-per-node=2GB" >> ./presto-server-0.226/etc/config.properties
echo "discovery-server.enabled=true" >> ./presto-server-0.226/etc/config.properties
echo "discovery.uri=http://sandbox-hdp.hortonworks.com:7001" >> ./presto-server-0.226/etc/config.properties

echo "com.facebook.presto=INFO" >> ./presto-server-0.226/etc/log.properties

sudo mv presto-server-0.226 /opt/presto/

# prepare the presto cli jar for execution
sudo mkdir -p /opt/presto/cli/
sudo mv presto-cli-0.226-executable.jar /opt/presto/cli/
sudo chmod a+x /opt/presto/cli/presto-cli-0.226-executable.jar

echo "/opt/presto/cli/presto-cli-0.226-executable.jar \"\$@\"" >> presto
chmod a+x presto
sudo mv presto /usr/bin/

#to start, run or stop presto server, use
sudo /opt/presto-server-0.226/bin/launcher start|run|stop

#to start presto client
presto --server sandbox-hdp.hortonworks.com:7001