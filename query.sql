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
			WHEN sub_category = 'DAIRY' THEN 'DAIRY PRODUCTS'
			WHEN sub_category = 'MEAT' THEN 'FRESH MEAT'
			WHEN sub_category = 'BAKERY' THEN 'BAKED GOODS'
			WHEN sub_category = 'PRODUCE' THEN 'FRESH PRODUCE'
			WHEN sub_category = 'SEAFOOD' THEN 'FRESH SEAFOOD'
			WHEN sub_category = 'BEVERAGES' THEN 'BEVERAGES'
			WHEN sub_category = 'SNACKS' THEN 'SNACKS'
			WHEN sub_category = 'FROZEN' THEN 'FROZEN FOODS'
			WHEN sub_category = 'CANNED' THEN 'CANNED GOODS'
			WHEN sub_category = 'CONDIMENTS' THEN 'CONDIMENTS/SAUCES'
			WHEN sub_category = 'SPICES' THEN 'SPICES AND HERBS'
			WHEN sub_category = 'INTERNATIONAL' THEN 'INTERNATIONAL FOODS'
			WHEN sub_category = 'GRAINS' THEN 'GRAINS AND PASTAS'
			WHEN sub_category = 'CLEANING' THEN 'CLEANING SUPPLIES'
			WHEN sub_category = 'HEALTH' THEN 'HEALTH AND WELLNESS'
			WHEN sub_category = 'BEAUTY' THEN 'PERSONAL CARE'
			WHEN sub_category = 'PETS' THEN 'PET SUPPLIES'
			WHEN sub_category = 'BABY' THEN 'BABY PRODUCTS'
			WHEN sub_category = 'MISC' THEN 'MISCELLANEOUS'
			ELSE sub_category || ' OTHERS'
		END AS grouped_category,
		SUM(gross_sales) AS revenue_in_category,
		COUNT(DISTINCT receipt_number) AS purchases_in_category
	FROM "database_name"."sales_data_table"
	WHERE transaction_date BETWEEN date '2023-04-01' AND date '2024-03-31'
		AND customer_id IS NOT NULL
	GROUP BY customer_id,
		CASE
			WHEN sub_category = 'DAIRY' THEN 'DAIRY PRODUCTS'
			WHEN sub_category = 'MEAT' THEN 'FRESH MEAT'
			WHEN sub_category = 'BAKERY' THEN 'BAKED GOODS'
			WHEN sub_category = 'PRODUCE' THEN 'FRESH PRODUCE'
			WHEN sub_category = 'SEAFOOD' THEN 'FRESH SEAFOOD'
			WHEN sub_category = 'BEVERAGES' THEN 'BEVERAGES'
			WHEN sub_category = 'SNACKS' THEN 'SNACKS'
			WHEN sub_category = 'FROZEN' THEN 'FROZEN FOODS'
			WHEN sub_category = 'CANNED' THEN 'CANNED GOODS'
			WHEN sub_category = 'CONDIMENTS' THEN 'CONDIMENTS/SAUCES'
			WHEN sub_category = 'SPICES' THEN 'SPICES AND HERBS'
			WHEN sub_category = 'INTERNATIONAL' THEN 'INTERNATIONAL FOODS'
			WHEN sub_category = 'GRAINS' THEN 'GRAINS AND PASTAS'
			WHEN sub_category = 'CLEANING' THEN 'CLEANING SUPPLIES'
			WHEN sub_category = 'HEALTH' THEN 'HEALTH AND WELLNESS'
			WHEN sub_category = 'BEAUTY' THEN 'PERSONAL CARE'
			WHEN sub_category = 'PETS' THEN 'PET SUPPLIES'
			WHEN sub_category = 'BABY' THEN 'BABY PRODUCTS'
			WHEN sub_category = 'MISC' THEN 'MISCELLANEOUS'
			ELSE sub_category || ' OTHERS'
		END AS grouped_category,
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
