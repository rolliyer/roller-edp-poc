-- models/sales/sales.sql
{{ config(materialized='table') }}

with source as (
    select * from {{ ref('sales_data') }}
),

enhanced as (
    select
        order_id,
        order_date,
        customer_id,
        product,
        category,
        region,
        quantity,
        unit_price,
        discount,
        quantity * unit_price as gross_amount,
        (quantity * unit_price) * (1 - discount) as net_amount,
        case
            when discount >= 0.15 then 'High Discount'
            when discount >= 0.05 then 'Medium Discount'
            else 'Low Discount'
        end as discount_band
    from source
)

select * from enhanced
