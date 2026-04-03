create database ecom;
use ecom;

# reduce repetition
CREATE OR REPLACE VIEW events_dedup AS
SELECT
    event_id,
    session_id,
    timestamp,
    event_type,
    product_id,
    qty,
    cart_size,
    payment,
    discount_pct,
    amount_usd
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY session_id, timestamp, event_type, product_id
               ORDER BY event_id
           ) AS rn
    FROM events
) t
WHERE rn = 1;
# check rows before and after
SELECT 
    (SELECT COUNT(*) FROM events) AS raw_events,
    (SELECT COUNT(*) FROM events_dedup) AS dedup_events;
    
# conversion
WITH session_funnel AS (
    SELECT
        session_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS has_view,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS has_checkout,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM events_dedup
    GROUP BY session_id
)
SELECT
    COUNT(*) AS total_sessions,
    SUM(has_view) AS view_sessions,
    SUM(has_cart) AS cart_sessions,
    SUM(has_checkout) AS checkout_sessions,
    SUM(has_purchase) AS purchase_sessions,
    ROUND(SUM(has_cart) / SUM(has_view), 4) AS view_to_cart_rate,
    ROUND(SUM(has_checkout) / SUM(has_cart), 4) AS cart_to_checkout_rate,
    ROUND(SUM(has_purchase) / SUM(has_checkout), 4) AS checkout_to_purchase_rate,
    ROUND(SUM(has_purchase) / SUM(has_view), 4) AS overall_conversion_rate
FROM session_funnel;

# conversion by different source
WITH session_funnel AS (
    SELECT
        s.source,
        e.session_id,
        MAX(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) AS has_view,
        MAX(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN e.event_type = 'checkout' THEN 1 ELSE 0 END) AS has_checkout,
        MAX(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM events_dedup e
    JOIN sessions s
      ON e.session_id = s.session_id
    GROUP BY s.source, e.session_id
)
SELECT
    source,
    COUNT(*) AS total_sessions,
    SUM(has_view) AS view_sessions,
    SUM(has_cart) AS cart_sessions,
    SUM(has_checkout) AS checkout_sessions,
    SUM(has_purchase) AS purchase_sessions,
    ROUND(SUM(has_cart) / NULLIF(SUM(has_view), 0), 4) AS view_to_cart_rate,
    ROUND(SUM(has_checkout) / NULLIF(SUM(has_cart), 0), 4) AS cart_to_checkout_rate,
    ROUND(SUM(has_purchase) / NULLIF(SUM(has_checkout), 0), 4) AS checkout_to_purchase_rate,
    ROUND(SUM(has_purchase) / NULLIF(SUM(has_view), 0), 4) AS overall_conversion_rate
FROM session_funnel
GROUP BY source
ORDER BY overall_conversion_rate DESC;

# conversion by different device
WITH session_funnel AS (
    SELECT
        s.device,
        e.session_id,
        MAX(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) AS has_view,
        MAX(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN e.event_type = 'checkout' THEN 1 ELSE 0 END) AS has_checkout,
        MAX(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM events_dedup e
    JOIN sessions s
      ON e.session_id = s.session_id
    GROUP BY s.device, e.session_id
)
SELECT
    device,
    COUNT(*) AS total_sessions,
    SUM(has_purchase) AS purchase_sessions,
    ROUND(SUM(has_purchase) / COUNT(*), 4) AS conversion_rate
FROM session_funnel
GROUP BY device
ORDER BY conversion_rate DESC;

# conversion ···

# orders and profits
# total orders
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(subtotal_usd), 2) AS total_subtotal,
    ROUND(SUM(total_usd), 2) AS total_revenue,
    ROUND(AVG(total_usd), 2) AS avg_order_value,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct
FROM orders;
# GMV trend
SELECT
    DATE_FORMAT(order_time, '%Y-%m') AS order_month,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_usd), 2) AS total_revenue,
    ROUND(AVG(total_usd), 2) AS avg_order_value
FROM orders
GROUP BY DATE_FORMAT(order_time, '%Y-%m')
ORDER BY order_month;

# discount invertion
SELECT
    CASE 
        WHEN discount_pct = 0 THEN 'No Discount'
        WHEN discount_pct > 0 AND discount_pct < 20 THEN 'Low Discount'
        WHEN discount_pct >= 20 AND discount_pct < 40 THEN 'Medium Discount'
        ELSE 'High Discount'
    END AS discount_group,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_usd), 2) AS total_revenue,
    ROUND(AVG(total_usd), 2) AS avg_order_value
FROM orders
GROUP BY discount_group
ORDER BY total_orders DESC;

# conversion under different discount
# useless cause there is no exact discount goods' click & add to cart info
SELECT
    CASE
        WHEN discount_pct IS NULL OR discount_pct = 0 THEN '0%'
        WHEN discount_pct > 0 AND discount_pct < 10 THEN '0-10%'
        WHEN discount_pct >= 10 AND discount_pct < 20 THEN '10-20%'
        WHEN discount_pct >= 20 AND discount_pct < 30 THEN '20-30%'
        ELSE '30%+'
    END AS discount_group,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
    SUM(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS checkouts,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
    ROUND(
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END), 0),
        4
    ) AS view_to_cart_rate,
    ROUND(
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END), 0),
        4
    ) AS view_to_purchase_rate,
    ROUND(
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END), 0),
        4
    ) AS cart_to_purchase_rate
FROM events
GROUP BY discount_group
ORDER BY discount_group;

# rating distribution
SELECT
    rating,
    COUNT(*) AS review_cnt
FROM reviews
GROUP BY rating
ORDER BY rating;

# low rating
SELECT
    p.product_id,
    p.name,
    p.category,
    COUNT(r.review_id) AS review_cnt,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM reviews r
JOIN products p
  ON r.product_id = p.product_id
GROUP BY p.product_id, p.name, p.category
HAVING COUNT(r.review_id) >= 5
ORDER BY avg_rating ASC, review_cnt DESC
LIMIT 20;

# high sales low rating
WITH sales AS (
    SELECT
        oi.product_id,
        SUM(oi.quantity) AS total_qty
    FROM order_items oi
    GROUP BY oi.product_id
),
ratings AS (
    SELECT
        product_id,
        COUNT(*) AS review_cnt,
        AVG(rating) AS avg_rating
    FROM reviews
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.name,
    p.category,
    s.total_qty,
    r.review_cnt,
    ROUND(r.avg_rating, 2) AS avg_rating
FROM products p
JOIN sales s
  ON p.product_id = s.product_id
JOIN ratings r
  ON p.product_id = r.product_id
WHERE r.review_cnt >= 5
 and s.total_qty >= 100 and r.avg_rating <= 3.5
ORDER BY s.total_qty DESC, avg_rating ASC
LIMIT 200;