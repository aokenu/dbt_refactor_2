with 

source as (

    select * from {{ source('jaffle_shop', 'jaffle_shop_orders') }}

),

transformed as (

    select

        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(id) as number_of_orders

    from source

    group by 1,2,3
)

select * from transformed