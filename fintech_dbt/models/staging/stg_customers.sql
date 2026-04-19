-- pull customers from raw table and rename columns to be more cleaner

with source as (
    select * from {{ source('fintech_oltp', 'customers') }}
),

renamed as (
    select
        customerid      as customer_id,
        fullname        as full_name,
        dob             as date_of_birth,
        gender,
        region,
        email,
        status          as customer_status,
        joindate        as join_date
    from source
)

select * from renamed
