WITH source AS (
    SELECT * FROM {{ref('stg_food_daily__order')}}
)

SELECT 
    * ,
    CASE 
        WHEN multiSearchAnyCaseInsensitive(feedback, ['food' , 'delicious' , 'stale' , 'taste' , 'salty' , 'postion']) > 0
        THEN 'Merchant (Food Quality)'
        WHEN multiSearchAnyCaseInsensitive(feedback, ['delivery' , 'rider' , 'driver' , 'doorstep' , 'late']) > 0
        THEN 'Platform(Logistics)'
        WHEN multiSearchAnyCaseInsensitive(feedback, ['app','coupon', 'code' , 'order' , 'checkout' , 'charge' , 'crash']) > 0
        THEN 'Platform(App & System)'
        WHEN multiSearchAnyCaseInsensitive(feedback, ['price' , 'expensive' , 'cheap' , 'worth' , 'cost'])
        THEN 'Pricing'
        ELSE 'Unclassified'
    END AS category
FROM source
