WITH source AS (
    SELECT * FROM {{ ref('stg_food_daily__order') }}
)

SELECT
    *,
    multiIf(
        feedback IN ('Stale food', 'Food not good'),
            'Merchant (Food Quality)',
        feedback IN ('Late delivery', 'Delivery boy didnt come at doorstep', 'Fast delivery'),
            'Platform(Logistics)',
        feedback IN ('Difficult to order', 'Complicated procedure', 'Coupon applied',
                     'Easy to order', 'Will order again'),
            'Platform(App & System)',
        feedback IN ('High price', 'Cheap and best', 'Worth'),
            'Pricing',
        feedback IN ('Awesome experience', 'Good service', 'Nice'),
            'Overall',
        'Unclassified'
    ) AS category,
    multiIf(
        feedback IN ('Stale food', 'Food not good', 'Late delivery',
                     'Delivery boy didnt come at doorstep', 'Difficult to order',
                     'Complicated procedure', 'High price', 'Coupon applied'),
            'Negative',
        feedback IN ('Fast delivery', 'Easy to order', 'Will order again',
                     'Cheap and best', 'Worth', 'Awesome experience',
                     'Good service', 'Nice'),
            'Positive',
        'Unclassified'
    ) AS feedback_sentiment
FROM source
