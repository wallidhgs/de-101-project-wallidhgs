/* 1. Query the top 5 sales by product */
WITH sales_by_product AS (
    SELECT
        c.product,
        SUM(s.sales) AS total_sales
    FROM sales_fact s
        LEFT JOIN color_dim c
            ON s.product = c.id
    GROUP BY c.product
)
SELECT TOP 5
    s.product,
    p.title,
    s.total_sales
FROM sales_by_product s
    LEFT JOIN product_dim p
        ON s.product = p.id
ORDER BY s.total_sales DESC;

/* 2. Query the top 5 sales by category agrupation */
WITH sales_by_category AS (SELECT
    p.category,
    SUM(s.sales) AS sum_category_sales
FROM sales_fact s
    LEFT JOIN color_dim c
        ON s.product = c.id
    LEFT JOIN product_dim p
        ON c.product = p.id
GROUP BY p.category)
SELECT TOP 5
    s.*
FROM sales_by_category s
ORDER BY s.sum_category_sales DESC;

/* 3. Query the least 5 sales by category agrupation */
WITH sales_by_category AS (SELECT
    p.category,
    SUM(s.sales) AS sum_category_sales
FROM sales_fact s
    LEFT JOIN color_dim c
        ON s.product = c.id
    LEFT JOIN product_dim p
        ON c.product = p.id
GROUP BY p.category)
SELECT TOP 5
    s.*
FROM sales_by_category s
ORDER BY s.sum_category_sales ASC;

/* 4. Query the top 5 sales by title and subtitle agrupation */
/* Since title and subtitle are always matching since both are product and not color properties, no need to group by subtitle, just join on t he final result*/
WITH sales_by_product AS (SELECT
    c.product,
    SUM(s.sales) AS sum_product_sales
FROM sales_fact s
    LEFT JOIN color_dim c
        ON s.product = c.id
    LEFT JOIN product_dim p
        ON c.product = p.id
GROUP BY c.product)
SELECT TOP 5
    p.title,
    p.subtitle,
    s.sum_product_sales
FROM sales_by_product s
    LEFT JOIN product_dim p
        ON s.product = p.id
ORDER BY s.sum_product_sales DESC;

/* 5. Query the top 3 products that has greatest sales by category */
WITH product_total_sales AS (
    SELECT
        c.product,
        SUM(s.sales) AS total_sales
    FROM sales_fact s
        LEFT JOIN color_dim c
            ON s.product = c.id
    GROUP BY c.product
),
product_category_sales_rank AS (
    SELECT
        p.category,
        ps.product,
        ps.total_sales,
        RANK() OVER (
            PARTITION BY p.category
            ORDER BY total_sales DESC
        ) AS category_rank
    FROM product_total_sales ps
        LEFT JOIN product_dim p
            ON ps.product = p.id
)
SELECT
    pc.category AS category_id,
    c.name AS category_name,
    p.id AS product_id,
    p.title AS product_name,
    pc.total_sales,
    pc.category_rank
FROM product_category_sales_rank pc
    LEFT JOIN category_dim c
        ON pc.category = c.id
    LEFT JOIN product_dim p
        ON pc.product = p.id
WHERE pc.category_rank <= 3
ORDER BY pc.category, pc.category_rank ASC;
