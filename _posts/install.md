---
layout: post
title: install LogIsland
------------------------


### Install logIsland

get [log-island-0.9.1.tgz]({{ site.baseurl }}/public/log-island-0.9.1.tgz) 
and unzip it where you desire on your edge node 
(which should be able to communicate with a spark cluster)

Make sure to get the logisland builded with your spark version on your cluster (1.4.1 in my case)
define LOGISLAND_HOME as you desire, here we'll just do a no permanent export

export LOGISLAND_HOME=path/to/unzipped-log-island

LogIsland need a SPARK_HOME to be defined on the machine where it is executed.