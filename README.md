# DOMAIN RESOLVER

Scala 2 version

## TOPICS

    bin/kafka-topics --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092 
    bin/kafka-topics --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_retry_1 --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092


## To start

    sbt "run /Users/ETORRIFUT/work/projects/domain-resolution-scala/conf-sample/application.conf"

## To prepare the package

    sbt dist