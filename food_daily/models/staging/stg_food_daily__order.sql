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
        restaurnt,
        Status as order_status,
        ratings,
        feedback
    from source
),

normalize as (
    SELECT
        trim(customer_id) as customer_id
        ,
        formatDateTime(parseDateTimeBestEffort(order_date), '%d-%m-%Y') as order_date,
        replace(order_time , '.' , ':') as order_time,
        trim(order_id) as order_id,
        arrayFilter(item -> item != '',
            arrayMap(
                item -> replaceRegexpAll(trim(lowerUTF8(item)), '[^a-zA-Z ]', ''),
                splitByChar(':', items[1])
                )
        ) AS items,
        
        CASE
            WHEN amount < 1 THEN  1
            ELSE amount
        END as amount,
        trim(concat(upper(substring(mode,1,1)), lower(substring(mode,2)))) as mode,
        trim(restaurnt) as restaurnt,
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
)




SELECT * from normalize



