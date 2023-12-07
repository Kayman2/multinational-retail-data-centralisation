======================================================================

#Milestone_4

======================================================================
Task 1 :

        SELECT
            country_code country,
            COUNT(*) total_no_stores
        FROM
            dim_store_details
        WHERE
            store_type ! = 'Web Portal'
        GROUP BY
            country_code
        ORDER BY
            2 desc;


==================================================================================================
Task 2 : 

        SELECT
            locality,
            COUNT(*) total_no_stores
        FROM
            dim_store_details
        GROUP BY
            locality
        HAVING
            COUNT(*) >= 10
        ORDER BY
            2 desc;

==================================================================================================
Task 3 :
        WITH sales_table AS (
            SELECT
                round(
                    CAST(SUM(ot.product_quantity * p.product_price) AS NUMERIC),
                    2
                ) AS total_sale,
                dt.month
            FROM
                dim_date_times dt
                INNER JOIN orders_table ot ON dt.date_uuid = ot.date_uuid
                INNER JOIN dim_products p ON p.product_code = ot.product_code
            GROUP BY
                MONTH
            ORDER BY
                1 desc
        )
        SELECT
            *
        FROM
            sales_table
        WHERE
            total_sale > (
                SELECT
                    AVG(total_sale)
                FROM
                    sales_table
            )

==================================================================================================
Task 4 : 
        SELECT
            COUNT(ot.*) numbers_of_sales,
            SUM(ot.product_quantity) product_quantity_count,
            CASE
                WHEN store_type = 'Web Portal' THEN 'Web'
                WHEN store_type <> 'Web Portal' THEN 'Offline'
            END AS location
        FROM
            dim_store_details sd
            INNER JOIN orders_table ot ON sd.store_code = ot.store_code
        GROUP BY
            location
        ORDER BY 1;
    

==================================================================================================

-- Task 5 
        WITH sales_table AS (
            SELECT
                sd.store_type AS store_type,
                SUM(ot.product_quantity * p.product_price) total_sales
            FROM
                dim_store_details sd
                INNER JOIN orders_table ot ON sd.store_code = ot.store_code
                INNER JOIN dim_products p ON p.product_code = ot.product_code
            GROUP BY
                sd.store_type
            ORDER BY
                2 desc
        )
        SELECT
            store_type,
            round(CAST(total_sales AS NUMERIC), 2) AS total_sales,
            round(CAST(total_sales * 100 / total AS NUMERIC), 2) AS "Percentage (%)" 
        FROM
            (
                SELECT
                    st.*,
                    SUM(total_sales) OVER() AS total
                FROM
                    sales_table st
            ) st

==================================================================================================
--- Task 6

        SELECT
            round(
                CAST(SUM(ot.product_quantity * p.product_price) AS NUMERIC),
                2
            ) AS total_sales,
            dt.year AS YEAR,
            dt.month AS MONTH
        FROM
            orders_table ot
            INNER JOIN dim_date_times dt ON ot.date_uuid = dt.date_uuid
            INNER JOIN dim_products p ON p.product_code = ot.product_code
        GROUP BY
            dt.month,
            dt.year
        ORDER BY
            total_sales desc
        LIMIT
            10

==================================================================================================
-- Task 7:  

SELECT
    SUM(staff_numbers) total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    2
ORDER BY
    1 desc;


==================================================================================================
-- Task 8: 

SELECT
    round(
        CAST(SUM(ot.product_quantity * p.product_price) AS NUMERIC),
        2
    ) total_sales,
    sd.store_type,
    MAX(country_code) AS country_code
FROM
    dim_store_details sd
    INNER JOIN orders_table ot ON ot.store_code = sd.store_code
    INNER JOIN dim_products p ON ot.product_code = p.product_code
WHERE
    sd.country_code = 'DE'
GROUP BY
    sd.store_type
ORDER BY total_sales ASC;

==================================================================================================
== Task_9 :
