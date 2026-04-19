-- customer dimension for the star schema
-- i also added age and how long they been a customer, thought it would be useful for analysis

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    full_name,
    date_of_birth,
    date_part('year', age(current_date, date_of_birth))::int  as age,
    gender,
    region,
    email,
    customer_status,
    join_date,
    date_part('year', age(current_date, join_date))::int      as years_as_customer
from customers
