echo "connector.name=hive-hadoop2" >> /opt/presto/etc/catalog/hive.properties
# get the hive.metastore.uri from Ambari's Hive config page
echo "hive.metastore.uri=thrift://sandbox-hdp.hortonworks.com:9083" >> /opt/presto/etc/catalog/hive.properties