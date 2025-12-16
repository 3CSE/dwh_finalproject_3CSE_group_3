## 6.5 Loading Layer

The Loading Layer is the final step in the ETL pipeline, where transformed and cleaned data is materialized from the virtual views in the `staging` schema into the physical, optimized tables of the `warehouse` schema. These scripts are designed to be idempotent, meaning they can be re-run without creating duplicate data or side effects. This process is orchestrated to run only after the transformation views have been successfully created and validated.

### Dimension Loading Strategy

Our dimension tables are designed to maintain a historical record of changes using a Slowly Changing Dimension (SCD) Type 2 methodology. This allows analysts to query the state of a dimension at any point in time. The loading scripts for dimensions like `DimProduct`, `DimCustomer`, `DimStaff`, and `DimMerchant` follow a consistent pattern to manage these changes.

The core of the SCD Type 2 logic involves three steps: detecting changes, expiring old records, and inserting new ones.

1.  **Change Detection:**
    The script first detects changes by comparing the latest record from the staging view against the current active record (`is_current = TRUE`) in the dimension table, joined on the stable Business Key (BK). Change detection is accomplished in one of two ways:
    -   **Attribute Hash Comparison:** For dimensions like `DimProduct` and `DimStaff`, a pre-computed `attribute_hash` is used. A mismatch between the source hash and the dimension hash indicates that at least one tracked attribute has changed.
    -   **Direct Attribute Comparison:** For dimensions like `DimCustomer` and `DimMerchant`, attributes are compared directly using `IS DISTINCT FROM`.

    The following snippet from `load_dim_product.sql` illustrates change detection using an attribute hash:
    ```sql
    -- DETECT CHANGES (from load_dim_product.sql)
    changes AS (
        SELECT
            s.*,
            d.product_key AS dim_key,
            d.product_attribute_hash AS dim_hash,
            d.product_bk AS existing_bk,
            (s.product_attribute_hash IS DISTINCT FROM d.product_attribute_hash) AS is_data_changed
        FROM latest_source_record s
        LEFT JOIN warehouse.DimProduct d 
            ON s.product_bk = d.product_bk
            AND d.is_current = TRUE
    ),
    ```

2.  **Expire Old Records:**
    If `is_data_changed` is true, the script updates the existing record in the dimension table, setting its `is_current` flag to `FALSE` and populating its `end_date` with the current timestamp. This effectively "closes" the old version of the record.

    ```sql
    -- EXPIRE OLD RECORDS (from load_dim_product.sql)
    deactivate_old AS (
        UPDATE warehouse.DimProduct d
        SET 
            is_current = FALSE,
            end_date = CURRENT_TIMESTAMP - INTERVAL '1 second'
        FROM changes c
        WHERE d.product_key = c.dim_key
          AND c.is_data_changed = TRUE
    )
    ```

3.  **Insert New and Changed Records:**
    Finally, the script inserts a new record into the dimension table for either brand-new entities (`existing_bk IS NULL`) or entities whose data has changed (`is_data_changed = TRUE`). This new record is marked with `is_current = TRUE`, its `effective_date` is set to the current timestamp, and its `end_date` is `NULL`, marking it as the currently active version.

Not all dimensions follow this pattern. `DimCampaign` is managed as an SCD Type 1, where changes simply overwrite existing values. `DimDate` is a static dimension populated procedurally by generating a continuous series of dates.

### Fact Loading Strategy

Fact table loading scripts are responsible for populating the central transactional tables, `FactOrder` and `FactOrderLineItem`. The primary challenge is to resolve the business keys from the source data into the correct surrogate keys from the dimension tables.

A critical aspect of this process is ensuring temporal correctness when joining with SCD Type 2 dimensions. The fact record must link to the version of the dimension that was active at the time the transaction occurred. This is achieved by joining not only on the key but also by checking that the fact's transaction date falls within the dimension record's `effective_date` and `end_date`.

The following snippet from `load_fact_order.sql` demonstrates this lookup logic:

```sql
-- FACT LOOKUPS (from load_fact_order.sql)
FROM orders o
...
-- Joins to dimensions
LEFT JOIN warehouse.DimCustomer cust 
    ON o.user_id = cust.user_id 
    AND o.transaction_date >= cust.effective_date 
    AND (cust.end_date IS NULL OR o.transaction_date < cust.end_date)

LEFT JOIN warehouse.DimMerchant merch 
    ON b.merchant_bk = merch.merchant_bk 
    AND o.transaction_date >= merch.effective_date 
    AND (merch.end_date IS NULL OR o.transaction_date < merch.end_date)
...
```

If a dimension key cannot be resolved (e.g., due to missing or dirty data), the script uses `COALESCE(cust.customer_key, -1)` to assign a default key of `-1`. This ensures referential integrity by pointing to the pre-defined "Unknown" member in each dimension table.

For idempotency, `FactOrder` uses an `ON CONFLICT DO UPDATE` clause to update metrics for existing orders, while `FactOrderLineItem` first deletes any line items for the orders being processed before inserting the new set.

### Section 13: Challenges & Solutions

This section covers specific challenges encountered during the design of the loading layer and the solutions implemented to address them.

#### Challenge 3: Historical Accuracy in Joins

**Problem:** When loading fact tables like `FactOrder`, we join with Slowly Changing Dimension (SCD) Type 2 tables (e.g., `DimCustomer`, `DimMerchant`). A simple join on a business key is insufficient because it doesn’t account for which version of a dimension record was active at the time of the transaction. For example, a customer might update their details, creating a new version of their record in `DimCustomer`. Orders placed before the change must link to the old record, and orders placed after must link to the new one.

**Solution:**
To ensure historical accuracy, the join condition between the fact table's source view and the dimension table includes a date range check. We use the `transaction_date` from the incoming order data and ensure it falls between the `effective_date` and `end_date` of the dimension record. This guarantees that each fact record is linked to the precise dimensional snapshot that was valid when the transaction occurred.

```sql
-- Point-in-time lookup from load_fact_order.sql
LEFT JOIN warehouse.DimCustomer cust
    ON o.user_id = cust.user_id
    AND o.transaction_date >= cust.effective_date
    AND (cust.end_date IS NULL OR o.transaction_date < cust.end_date)
```

#### Challenge 4: Idempotency

**Problem:** ETL pipelines must be runnable multiple times without causing data duplication or errors. If a fact-loading script is re-executed, it could either fail due to primary key violations or create duplicate records, leading to incorrect analytics. The scripts must be designed to be idempotent, meaning they produce the same outcome regardless of how many times they are run.

**Solution:**
We implemented two different strategies for idempotency in our fact-loading scripts.

1.  **Upsert with `ON CONFLICT`:** For the `FactOrder` table, which has a unique `order_id`, we use PostgreSQL's `ON CONFLICT DO UPDATE` clause. If an incoming order record has an `order_id` that already exists in the table, the script doesn't attempt a new insert. Instead, it updates the existing record with the new values. This is an atomic and efficient way to handle re-runs.

    ```sql
    -- Idempotency in load_fact_order.sql
    ON CONFLICT (order_id)
    DO UPDATE SET
        order_total_amount = EXCLUDED.order_total_amount,
        net_order_amount = EXCLUDED.net_order_amount,
        -- ... other fields
        availed_flag = EXCLUDED.availed_flag;
    ```

2.  **Transactional Delete and Insert:** The `FactOrderLineItem` table does not have a simple unique key per row, as it represents a one-to-many relationship with `FactOrder`. For this table, we first delete all existing line items that belong to the orders being processed in the current batch. Then, we insert the new set of line items. This entire `DELETE` and `INSERT` process is wrapped in a `BEGIN...COMMIT` transaction block to ensure atomicity—if any part of the operation fails, the entire transaction is rolled back, preventing partial data loads.

    ```sql
    -- Idempotency in load_fact_order_line_item.sql
    BEGIN;

    DELETE FROM warehouse.FactOrderLineItem
    WHERE order_id IN (
        SELECT DISTINCT order_id
        FROM staging.view_clean_line_items_products
        WHERE order_id IS NOT NULL
    );

    INSERT INTO warehouse.FactOrderLineItem (...)
    SELECT ... ;

    COMMIT;
    ```