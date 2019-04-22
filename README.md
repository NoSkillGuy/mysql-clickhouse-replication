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

# Creating ClickHouse User
```
cat << EOF
<?xml version="1.0"?>
<yandex>
    <users>
        <mysql_clickhouse_slave>
            <password>$MYSQL_CLICKHOUSE_PASSWORD</password>
            <networks incl="networks" replace="replace">
                <ip>::/0</ip>
            </networks>

            <!-- Settings profile for user. -->
            <profile>default</profile>

            <!-- Quota for user. -->
            <quota>default</quota>
        </mysql_clickhouse_slave>
    </users>
</yandex>
EOF
```

# prerequisites

1. Mysql (refer: https://dev.mysql.com/downloads/mysql/)
2. Clickhouse (refer: https://clickhouse.yandex/docs/en/getting_started/)

# Installation

1. git clone https://github.com/NoSkillGuy/mysql-clickhouse-replication.git
2. cd mysql-clickhouse-replication
3. python3 setup.py install