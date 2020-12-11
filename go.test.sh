#!/usr/bin/env bash

## create table
curl "localhost:8123" -d 'DROP TABLE IF EXISTS test1'
curl "localhost:8123" -d 'CREATE TABLE test1
(
    `day` Date DEFAULT toDate(time),
    `time` DateTime,
    `name` String,
    `value` Float64
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (time, name)'

curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_auto_schema'
curl "localhost:8123" -d 'CREATE TABLE test_auto_schema AS test1'

curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_dynamic_schema'
curl "localhost:8123" -d 'CREATE TABLE test_dynamic_schema AS test1'

## send the messages to kafka
now=`date --rfc-3339=ns`
for i in `seq 1 10000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i }"
done > a.json
now=`date --rfc-3339=ns`
for i in `seq 10001 20000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey1\" : $i }"
done >> a.json
now=`date --rfc-3339=ns`
for i in `seq 20001 30000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey2\" : $i.123 }"
done >> a.json
now=`date --rfc-3339=ns`
for i in `seq 30001 100000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey3\" : \"name$i\" }"
done >> a.json
echo "generated a.json"
echo "cat /tmp/a.json | kafka-console-producer --topic topic1 --broker-list localhost:9092" > send.sh
sudo docker cp a.json kafka:/tmp/
sudo docker cp send.sh kafka:/tmp/
sudo docker exec kafka sh /tmp/send.sh

## start clickhouse_sinker to consume
timeout 60 ./dist/clickhouse_sinker --local-cfg-file docker/config.json

## check result
count=`curl "localhost:8123" -d 'SELECT count() FROM test1'`
echo "Got test1 count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

schema=`curl "localhost:8123" -d 'DESC test_dynamic_schema' 2>/dev/null | grep newkey | sort | tr -d '\t' | tr '\n' ','`
echo "Got test_dynamic_schema schema => $schema"
[ $schema = "newkey1Nullable(Int64),newkey2Nullable(Float64),newkey3Nullable(String)," ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1


## reset kafka consumer-group offsets
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_sinker --all-topics --to-earliest
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_auto_schema --all-topics --to-earliest
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_dynamic_schema --all-topics --to-earliest

## truncate tables
curl "localhost:8123" -d 'TRUNCATE TABLE test1'
curl "localhost:8123" -d 'TRUNCATE TABLE test_auto_schema'
curl "localhost:8123" -d 'TRUNCATE TABLE test_dynamic_schema'

## publish clickhouse_sinker config
./dist/nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --local-cfg-file docker/config.json

## start clickhouse_sinker to consume
timeout 30 ./dist/clickhouse_sinker --nacos-register-enable --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos

## check result
count=`curl "localhost:8123" -d 'SELECT count() FROM test1'`
echo "Got test1 count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1
