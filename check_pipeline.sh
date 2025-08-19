#!/bin/bash

PROJECT_DIR="/mnt/winpart/Khanh/project_cdc_mysql_to_postgres"
KAFKA_CONNECT_CONTAINER="project_cdc_mysql_to_postgres-kafka-connect-1"
MYSQL_CONTAINER="project_cdc_mysql_to_postgres-mysql-1"
KAFKA_BOOTSTRAP="kafka:9092"
TOPIC="mysql_server.source_db.users"
HISTORY_TOPIC="schema-changes.source_db"


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color


print_header(){
    echo -e "${YELLOW}=== $1  ===${NC}"
}

#1> check  mysql binary  logging
print_header "Check mysql  binary logging"
docker exec  -it $MYSQL_CONTAINER mysql  -u root -proot  -e "show VARIABLES LIKE 'log_bin';"||
{
    echo -e "${RED}ERROR: faided to check  log_bin  .Ensure MYSQL  is running  and root password  is correct. ${NC}"
}
docker exec -it $MYSQL_CONTAINER mysql -u root -proot -e "SHOW VARIABLES LIKE 'binlog_format';" || {
  echo -e "${RED}ERROR: Failed to check binlog_format.${NC}"
}
docker exec -it $MYSQL_CONTAINER mysql -u root -proot -e "SHOW VARIABLES LIKE 'server_id';" || {
  echo -e "${RED}ERROR: Failed to check server_id.${NC}"
}
docker exec -it $MYSQL_CONTAINER mysql -u root -proot -e "SHOW BINARY LOGS;" || {
  echo -e "${RED}ERROR: Failed to check binary logs. Ensure root has REPLICATION CLIENT privilege.${NC}"
}

# 2. Check MySQL user permissions
print_header "Checking MySQL User Permissions"
docker exec -it $MYSQL_CONTAINER mysql -u root -proot -e "SHOW GRANTS FOR 'root'@'%';" || {
  echo -e "${RED}ERROR: Failed to check permissions for root.${NC}"
}


# 3. Check Kafka connection
print_header "Checking Kafka Connection"
docker exec -it $KAFKA_CONNECT_CONTAINER /kafka/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP || {
  echo -e "${RED}ERROR: Failed to connect to Kafka at $KAFKA_BOOTSTRAP.${NC}"
}



# 4. Check if dbhistory.source_db topic exists
print_header "Checking Kafka Topic dbhistory.source_db"
docker exec -it $KAFKA_CONNECT_CONTAINER /kafka/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP | grep $HISTORY_TOPIC || {
  echo -e "${YELLOW}WARNING: Topic $HISTORY_TOPIC not found. Creating it...${NC}"
  docker exec -it $KAFKA_CONNECT_CONTAINER /kafka/bin/kafka-topics.sh --create --bootstrap-server $KAFKA_BOOTSTRAP --topic $HISTORY_TOPIC --partitions 1 --replication-factor 1
}

# #4.1.delete  connect kafka

# print_header "delect connect kafka"
# docker exec -it $KAFKA_CONNECT_CONTAINER curl  -X DELETE http://localhost:8083/connectors/mysql-connector || {
#   echo -e"${RED}error : Failed delete  conenct kafka > ensure kafka connect is running.${NC}"
# }


#4.2 . connector kafka 
print_header "conenct kafka connect"
docker exec -it $KAFKA_CONNECT_CONTAINER curl -X POST -H "Content-Type: application/json" --data @/kafka/connect/debezium-connector/mysql-connector.json http://localhost:8083/connectors || {
  echo -e"${RED}error : Failed conenct kafka > ensure kafka connect is running.${NC}"
}

# 5. Check connector status
print_header "Checking Kafka Connect Connector Status"
docker exec -it $KAFKA_CONNECT_CONTAINER curl -s -X GET http://localhost:8083/connectors/mysql-connector/status | jq . || {
  echo -e "${RED}ERROR: Failed to check connector status. Ensure Kafka Connect is running.${NC}"
}


# 6. Check data in topic
print_header "Checking Data in Topic $TOPIC"
docker exec -it $KAFKA_CONNECT_CONTAINER /kafka/bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_BOOTSTRAP --topic $TOPIC --from-beginning --max-messages 50 --timeout-ms 10000 || {
  echo -e "${RED}ERROR: Failed to consume messages from $TOPIC. Topic may be empty or consumer timed out.${NC}"
}


# 7. Check MySQL data
print_header "Checking MySQL Data in source_db.users"
docker exec -it $MYSQL_CONTAINER mysql -u root -proot -e "SELECT count(*) FROM source_db.users;" || {
  echo -e "${RED}ERROR: Failed to query source_db.users. Ensure table exists and root has access.${NC}"
}



# 8. Check Kafka Connect logs (last 20 lines)
print_header "Checking Kafka Connect Logs (Last 20 Lines)"
docker logs --tail 50 $KAFKA_CONNECT_CONTAINER || {
  echo -e "${RED}ERROR: Failed to retrieve Kafka Connect logs.${NC}"
}


echo -e "${GREEN}=== Check Completed ===${NC}"

