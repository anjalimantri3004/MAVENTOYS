--SELECT * FROM MAVENTOYS.PUBLIC.SALES
SELECT
  CAST("Sale_ID" AS INTEGER) AS sale_id,
  TO_DATE("Date", 'YYYY-MM-DD') AS sale_date,
  CAST("Store_ID" AS INTEGER) AS store_id,
  CAST("Product_ID" AS INTEGER) AS product_id,
  CAST("Units" AS INTEGER) AS units
FROM {{ ref('sales') }}
WHERE
  "Sale_ID" IS NOT NULL
  AND "Date" IS NOT NULL
  AND "Store_ID" IS NOT NULL
  AND "Product_ID" IS NOT NULL
  AND "Units" > 0