with source as (
    SELECT * from {{ source('food_daily','food_daily') }}
),

rename as (
    SELECT
        Customer_id as customer_id,
        date as order_date,
        time as order_time,
        order_id,
        items,
        amount,
        mode,
        restaurnt as restaurant,
        Status as order_status,
        ratings,
        feedback
    from source
),

normalize as (
    SELECT
        trim(customer_id) as customer_id
        ,
        formatDateTime(parseDateTimeBestEffort(order_date), '%Y-%m-%d') as order_date,
        replace(order_time, '.', ':') as order_time,
        trim(order_id) as order_id,
        arrayFilter(item -> item != '',
            arrayMap(
                item -> replaceRegexpAll(trim(lowerUTF8(item)), '[^a-zA-Z ]', ''),
                splitByChar(':', items[1])
                )
        ) AS items,
        amount,
        trim(concat(upper(substring(mode,1,1)), lower(substring(mode,2)))) as mode,
        trim(restaurant) as restaurant,
        multiIf(
            trimBoth(lower(order_status)) = 'delivered','Delivered',
            trimBoth(lower(order_status)) = 'cancelled' , 'Cancelled',
            trimBoth(lower(order_status)) = 'not delivered' , 'Not delivered',
            trimBoth(lower(order_status)) = 'on hold' , 'On Hold',
            order_status
        ) as order_status,
        CASE
            WHEN ratings < 1 THEN  1
            WHEN ratings > 5 THEN 5
            ELSE  ratings
        END as ratings,
        trim(feedback) as feedback
    from rename
),

conversion_type as (
    SELECT
        customer_id,
        toDate(order_date) as order_date,
        order_time::Nullable(String) as order_time,
        upper(substring(hex(cityHash64(concat(
            customer_id, order_date, order_time, toString(amount),
            arrayStringConcat(items, ':'), restaurant, toString(mode), order_status , ratings , feedback
        ))), 1, 5)) as order_id,
        items,
        mode::Enum8('Online' = 1 , 'Cash' = 2 , 'Wallet' = 3 ,'Card' = 4) as mode,
        restaurant,
        amount,
        order_status::Enum8('Delivered' = 1 , 'Cancelled' = 2 , 'Not delivered' = 3 , 'On Hold'=4) as order_status,
        ratings,
        feedback
    from normalize
),

check_date as (
    select distinct * from conversion_type
)

SELECT * from check_date
