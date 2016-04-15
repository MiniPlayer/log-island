---
layout: post
title: parse apache logs
------------------------

### Requirement

    A Spark cluster, or a sparkmaster in standalone mode (see [spark documentation](http://spark.apache.org/documentation.html))
    A Kafka cluster, at least one broker (see [kafka documentation](http://kafka.apache.org/documentation.html))
    A zookepeer cluster or in standalone mode (see [zookeeper documentation](https://zookeeper.apache.org/))
    A elasticseach cluster or in standalone mode (see [elasticsearch documentation](https://www.elastic.co/guide/index.html))
    A nifi cluster or in standalone mode (see [nifi documentation](https://nifi.apache.org/docs.html))
    
### Install logIsland

[install](#install.md)


### Launch the parser stream

## Launch the syslog parser log

Make sure tou have SPARK_HOME well defined, logIsland will use it.
Then you can launch the stream job as follow:

    nohup $LOGISLAND_HOME/bin/log-parser \
        --kafka-brokers $KAFKA_BROKER_HOSTS:6667 \
        --input-topics syslogs \
        --output-topics syslogs_event \
        --max-rate-per-partition 10000 \
        --log-parser com.hurence.logisland.plugin.apache.ApacheLogParser \
        --zk-quorum $ZOOKEEPER_HOSTS > syslog_parser.log &
    
This job will parse logs from the topic 'syslogs'
 into events to the topic 'syslog_event' en mode 'streaming'.
 

### Launch the event indexer stream
 
In our case we will use elastic search. Make sure your elasticsearch plugin
in logIsland is compiled with the same version that your ES cluster. 
 
     nohup $LOGISLAND_HOME/bin/event-indexer \
         --kafka-brokers $KAFKA_BROKER_HOSTS:6667 \
         --es-cluster $ES_NAME_CLUSTER \
         --es-host $ES_BROKER_HOSTS \
         --index-name syslogs \
         --input-topics syslogs_event \
         --max-rate-per-partition 10000 \
         --event-mapper com.hurence.logisland.plugin.apache.SyslogEventMapper \
         --zk-quorum $ZOOKEEPER_HOSTS > syslog_indexer_es.log &
 
 
### Set up the syslog flow
 
## Configure your machines which you want to index the syslogs
   

First, you want to send all your syslogs into your nifi cluster.
So modify your machine to send the log on a nifi node at a specific port that you are free to chose
, we'll call it $SYSLOG_PORT.
In my case my machine are using rsyslog to generate these logs. If you are
using another daemon, please refer to the according documentation.
For rsyslog, modify your conf file. It is probably at : '/etc/rsyslog.conf'
and add this line 
'*.*;                            @@$NIFI_NODE_IP:SYSLOG_PORT'
it will send all logs managed by rsyslog at the specified adress, '@@'
mean that the protocol used is tcp. Restart your syslog service

    service rsyslog restart

Do this for each machine you want to index logs.
 
## Configure Nifi flow
 
**Requirement:** a nifi cluster running or a standalone nifi.

Connect to your nifi UI at nifi.machine.ip.adress:8080/nifi
or the port your nifi is runnning with

You want to collect all the syslogs that you send. You may have chose
different port for each of your machine but we'll assume you followed this tutorial.

Create A ListenSyslog processor and modify these properties:
Protocol => TCP
Port => $SYSLOG_PORT
Message Delimiter => \n

Create A PutKafka processor and modify these properties:
Known Brokers => $KAFKA_BROKER_HOSTS:6667
Topic Name => syslogs
Message Delimiter => \n
Client Name => logisland

    **Note**: You may have to complete other properties for the processors
    to be runnable. (see nifi doc)

You have to end all flow in nifi before starting a flow. So You can,
 for example:
- a self connection in your putKafkaProcessor for invalid Data.
- auto terminate success in Settings tab of your putKafkaProcessor.
- auto terminate failure for your ListenSyslog Processor.

   Run your workFlow !

### Look your data !!

Your data will be indexed in a real-time flux into ES ! You can look 
at your data with pluggins of elasticsearch, kibana or others. Enjoy.


