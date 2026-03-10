#!/bin/bash
#===============================================================================
# Configuration file for sqlserver_to_hdfs_sqoop.sh
#===============================================================================

# Database Configuration
DB_SERVER="sqlserver.company.com"
DB_PORT="1433"
DB_NAME="production_db"
DB_USER="sqoop_user"
# Better to use password file or environment variables
DB_PASSWORD_FILE="/home/username/.sqoop_pass"

# Target Configuration
HDFS_BASE_PATH="/data/landing"
LOCAL_BASE_PATH="/data/processed"

# Table Mappings (can be used for batch processing)
declare -A TABLE_CONFIG=(
    ["customers"]="id < 1000|500|customer_data"
    ["orders"]="order_date >= '2024-01-01'|1000|order_data"
    ["products"]="active = 1|all|product_catalog"
)

# Default Query Parameters
DEFAULT_LIMIT="1000"
DEFAULT_NULL_STRING='\\N'

# Logging Configuration
LOG_RETENTION_DAYS=30
SUMMARY_RETENTION_DAYS=7