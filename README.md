# CreditCardFraud

This are steps to run the Credit Card Fraud Detection System Application. MAC Operating System is used for developing and run the application,

Step 1: Install MYSQL Server(Version 8) and Client, Kafka(version 2.12-0.11.0.2), Prometheus(Version 2.9.2), Grafana(Version 6.1.6)

Step 2: Make MySQL database server up.

Step 3: Go to bin folder and make ZooKeeper server up  using this command ./zookeeper-server-start.sh ../config/zookeeper.properties

Step 4: Goto bin folder and make Kafka server up using this command ./kafka-server-start.sh ../config/server.properties

Step 5: Goto bin folder and create Kafka topic using command ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic transactionPublish

Step 6: Goto bin folder and make Prometheus server up using command Prometheus server start ./prometheus --config.file config/prometheus.yml

Step 7: Goto bin folder and make Grafana server up using command ./grafanaserver 

Step 8: Import CreditCardFraud, creditcard-dashboard-service, creditcard-transactions-kafka-comsumer, creditcard-transactions-kafka-producer projects in STS or Eclipse.

Step 9: In CreditCardFraud project, in src/main/java and in package com.myhu.cheela.spark, open ImportBasicDataToMysql.java and right click on that class and click Run as Java Application.

Step 10: In CreditCardFraud project, in src/main/java and in package com.myhu.cheela.model open CreditCardMachineLearningModel.java and right click on that class and click Run as Java Application. 

Step 11: In creditcard-transactions-kafka-producer project, in src/main/java and in package com.myhu.cheela open CreditcardTransactionsKafkaProducerApplication.java and right click on that class and click Run as Java Application. 

Step 12: In creditcard-transactions-kafka-comsumer project, in src/main/java and in package com.myhu.cheela.main open CreditcardTransactionsKafkaProducerApplication.java and right click on that class and click Run as Java Application.

Step 13: Hit the creditcard-transactions-kafka-producer project with url http://localhost:8080/creditcard/v1/publish

Step 14: In creditcard-dashboard-service project, in src/main/java and in package com.myhu.cheela, open CreditcardDashboardServiceApplication.java and right click on that class and click Run as Java Application.

Step 15: Open browser and start the dashboard http://localhost:3000


