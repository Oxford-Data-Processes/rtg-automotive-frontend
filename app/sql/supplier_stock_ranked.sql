CREATE TABLE supplier_stock_ranked AS
WITH ranked_supplier_stock AS (
    SELECT
        ps.part_number,
        ps.supplier,
        ps.quantity,
        ps.updated_date,
        ps.custom_label,
        ROW_NUMBER() OVER (PARTITION BY ps.supplier, ps.custom_label ORDER BY ps.updated_date DESC) AS rn
    FROM
        supplier_stock ps
)

SELECT
    r.custom_label,
    r.quantity,
    COALESCE(
        MAX(CASE WHEN r.rn = 1 THEN r.quantity END) -
        MAX(CASE WHEN r.rn = 2 THEN r.quantity END),
        MAX(CASE WHEN r.rn = 1 THEN r.quantity END)
    ) AS quantity_delta,
    MAX(r.updated_date) AS updated_date,
    r.supplier
FROM
    ranked_supplier_stock r
GROUP BY
    r.custom_label, r.supplier;