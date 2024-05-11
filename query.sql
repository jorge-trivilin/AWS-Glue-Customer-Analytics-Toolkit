UNLOAD (
WITH total_customer_revenue AS (
	SELECT customer_id,
		SUM(gross_sales) AS total_revenue
	FROM "database_name"."sales_data_table"
	WHERE transaction_date BETWEEN date '2023-04-01' AND date '2024-03-31'
		AND customer_id IS NOT NULL
	GROUP BY customer_id
),
total_customer_purchases AS (
	SELECT customer_id,
		COUNT(DISTINCT receipt_number) AS total_purchases
	FROM "database_name"."sales_data_table"
	WHERE transaction_date BETWEEN date '2023-04-01' AND date '2024-03-31'
		AND customer_id IS NOT NULL
	GROUP BY customer_id
),
revenue_purchases_by_category AS (
	SELECT customer_id,
		CASE
			WHEN top_level_category = 'PHARMACY' THEN top_level_category
			WHEN sub_category = 'FOOD' THEN sub_category
			WHEN sub_category = 'GENERAL GOODS' THEN sub_category
			WHEN sub_category = 'BEAUTY' THEN sub_category
			WHEN sub_category = 'SKINCARE' THEN sub_category
			WHEN sub_category = 'HYGIENE' THEN sub_category
			WHEN sub_category = 'CHILDREN' THEN sub_category
			WHEN sub_category = 'HEALTH' THEN sub_category
			WHEN sub_category = 'OTC'
			and sub_sub_category = 'PAIN AND FEVER' THEN 'OTC ' || sub_sub_category
			WHEN sub_category = 'OTC'
			and sub_sub_category = 'DIGESTIVE HEALTH' THEN 'OTC ' || sub_sub_category
			WHEN sub_category = 'OTC'
			and sub_sub_category = 'COLD AND FLU' THEN 'OTC ' || sub_sub_category
			WHEN sub_category = 'OTC'
			and sub_sub_category = 'EYE CARE' THEN 'OTC ' || sub_sub_category
			WHEN sub_category = 'OTC'
			and sub_sub_category = 'VITAMINS AND MINERALS' THEN 'OTC ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'ALLERGY' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'ANTIBIOTIC' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'ANTI-INFLAMMATORY' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'DERMATOLOGICAL' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'DIABETES' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'CARDIOVASCULAR SYSTEM' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'GENITAL HORMONAL SEX' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'NERVOUS SYSTEM' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'RESPIRATORY SYSTEM' THEN 'CONTINUOUS RX ' || sub_sub_category
			WHEN sub_category = 'PRESCRIPTION'
			and sub_sub_category = 'VITAMINS AND MINERALS' THEN 'CONTINUOUS RX ' || sub_sub_category
			when sub_category = 'MISC PROMOTIONS' then 'OTHERS'
			when sub_category = 'COMPOUNDED' then 'OTHERS'
			when sub_category = 'UNCLASSIFIED' then 'OTHERS'
			when sub_category = 'SERVICE' then 'SERVICE OTHERS' ELSE sub_category || ' OTHERS'
		END AS grouped_category,
		SUM(gross_sales) AS revenue_in_category,
		COUNT(DISTINCT receipt_number) AS purchases_in_category
	FROM "database_name"."sales_data_table"
	WHERE transaction_date BETWEEN date '2023-04-01' AND date '2024-03-31'
		AND customer_id IS NOT NULL
	GROUP BY customer_id,
		CASE
			/* Category case conditions are similar to those described above */
		END
)
SELECT rcc.customer_id,
	rcc.grouped_category,
	rcc.revenue_in_category,
	rcc.purchases_in_category
FROM revenue_purchases_by_category rcc
	JOIN total_customer_revenue tcr ON rcc.customer_id = tcr.customer_id
	JOIN total_customer_purchases tcp ON rcc.customer_id = tcp.customer_id
	) 
	TO 's3://data-store-path/processed-data/fiscal-year=2024/' 
	WITH (format = 'PARQUET', compression = 'SNAPPY')
