#!/bin/bash

# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username "$ADMIN_USERNAME" --firstname "$ADMIN_FIRSTNAME" --lastname "$ADMIN_NAME" --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

# Add Dashboard
# Not Working because of postgres password
#superset import-dashboards --path /data/dashboard.zip --username admin


# Starting server
/bin/sh -c /usr/bin/run-server.sh