#!/bin/bash
# run this first before doing anything with dbt

export DBT_HOST=ep-frosty-math-anosahcy-pooler.c-6.us-east-1.aws.neon.tech
export DBT_USER=neondb_owner
export DBT_PASSWORD=npg_g5hVQRvYS6uk

echo "credentials loaded"
echo "now run: dbt debug"
