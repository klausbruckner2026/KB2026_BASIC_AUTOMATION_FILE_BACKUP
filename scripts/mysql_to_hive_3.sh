#!/bin/bash
# Configuration file for mysql_to_hive script

# MySQL Settings
MYSQL_USER="root"
MYSQL_PASSWORD="root"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"

# Database Mappings
declare -A TABLE_MAPPINGS=(
    ["sales"]="hive_sales_table"
    ["customers"]="hive_customers_table"
    ["products"]="hive_products_table"
)

# Hive Settings
HIVE_WAREHOUSE_DIR="/user/hive/warehouse"
HIVE_DATABASE="default"

# Date Settings
DATE_COLUMN="created_at"  # Default date column

# Alert Settings
ALERT_EMAIL="data-team@example.com"
SLACK_WEBHOOK="https://hooks.slack.com/services/xxx/yyy/zzz"