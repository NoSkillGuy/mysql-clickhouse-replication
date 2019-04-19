# mysql-clickhouse-replication
GOAL: To create a ClickHouse slave for Mysql with real time replication

# Creating a Mysql User with Permissions
```
MYSQL_CLICKHOUSE_USER='mysql_clickhouse_slave'
MYSQL_CLICKHOUSE_PASSWORD=$(openssl rand -base64 12)
echo "Run the below commands in Mysql Console"
echo "CREATE USER '$MYSQL_CLICKHOUSE_USER'@'localhost' IDENTIFIED BY '$MYSQL_CLICKHOUSE_PASSWORD';"
echo "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '$MYSQL_CLICKHOUSE_USER'@'localhost';"
```
# prerequisites

1. Mysql (refer: https://dev.mysql.com/downloads/mysql/)
2. Clickhouse (refer: https://clickhouse.yandex/docs/en/getting_started/)

# Installation

1. git clone https://github.com/NoSkillGuy/mysql-clickhouse-replication.git
2. cd mysql-clickhouse-replication
3. python3 setup.py install