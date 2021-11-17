
{{ config(materialized='table') }}

with source as (

    select * from {{ source('coffee_shop', 'customers') }}

),

renamed as (

    select
        id,
        name,
        email

    from source

)

select * from renamed