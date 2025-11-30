-- Inventory_management_Analysis 
 
 -- A.	Efficiency KPIs(Inventory Turnover Ratio,Days of Inventory (DOI) / Days Inventory Outstanding (DIO),
 -- Sell-Through Rate (STR))

USE inventory_db;
-- KPI Queries for Inventory_db (based on provided schema)
-- Assumptions:
-- 1) sales.ProductID links to products_master.ProductID and inventory_transactions.ProductID
-- 2) inventory_transactions.StockLevelAfter reflects inventory units after that transaction
-- 3) inventory_transactions.UnitCost is the cost at that transaction (used to approximate COGS)
-- 4) If UnitCost is missing in inventory_transactions, queries fall back to products_master.UnitCost
-- 5) Define period by setting @start_date and @end_date (inclusive)

-- =========================
-- Parameters (edit these dates for the analysis period)
-- =========================
CREATE VIEW Turnover_ratio AS
WITH sales_with_cost AS (
    SELECT
        s.ProductID,
        s.Quantity,
        s.Date,
        (
            SELECT it.UnitCost
            FROM inventory_transactions it
            WHERE it.ProductID = s.ProductID
              AND it.TransactionType = 'Purchase'
              AND it.Date <= s.Date
            ORDER BY it.Date DESC
            LIMIT 1
        ) AS cost_per_unit
    FROM sales s
    WHERE YEAR(s.Date) = 2023
),
cogs_calc AS (
    SELECT COALESCE(SUM(s.Quantity * s.cost_per_unit),0) AS COGS
    FROM sales_with_cost s
    WHERE s.cost_per_unit IS NOT NULL
),
monthly_inventory AS (
    SELECT
        x.ProductID,
        x.StockLevelAfter,
        x.last_date,
        COALESCE(
            (
                SELECT it.UnitCost
                FROM inventory_transactions it
                WHERE it.ProductID = x.ProductID
                  AND it.TransactionType = 'Purchase'
                  AND it.Date <= x.last_date
                ORDER BY it.Date DESC
                LIMIT 1
            ),
            pm.UnitCost
        ) AS unit_cost
    FROM (
        SELECT
            it.ProductID,
            it.StockLevelAfter,
            it.Date AS last_date,
            ROW_NUMBER() OVER (
                PARTITION BY it.ProductID, DATE_FORMAT(it.Date,'%Y-%m')
                ORDER BY it.Date DESC
            ) AS rn
        FROM inventory_transactions it
        WHERE YEAR(it.Date) = 2023
    ) x
    JOIN products_master pm ON x.ProductID = pm.ProductID
    WHERE x.rn = 1
      AND x.StockLevelAfter > 0
),
avg_inventory AS (
    SELECT COALESCE(AVG(mi.StockLevelAfter * mi.unit_cost),0) AS Average_Inventory_Value
    FROM monthly_inventory mi
)
SELECT
    ROUND(c.COGS,2) AS COGS,
    ROUND(a.Average_Inventory_Value,2) AS Average_Inventory_Value,
    ROUND(c.COGS / NULLIF(a.Average_Inventory_Value,0),2) AS Inventory_Turnover_Ratio,
    ROUND(365 / NULLIF(c.COGS / a.Average_Inventory_Value,0),1) AS Days_in_Inventory
FROM cogs_calc c
CROSS JOIN avg_inventory a;

-- ==============================================================
-- Sell-Through Rate (STR) 
-- ==============================================================
CREATE OR REPLACE VIEW SellThrough_Rate AS
WITH
-- 1️⃣ إجمالي الوحدات المباعة لكل منتج في السنة المحددة
sales_summary AS (
    SELECT
        s.ProductID,
        SUM(s.Quantity) AS Total_Sold
    FROM sales s
    WHERE YEAR(s.Date) = 2023
    GROUP BY s.ProductID
),

-- 2️⃣ إجمالي الوحدات المشتراة لكل منتج في نفس السنة
purchases_summary AS (
    SELECT
        it.ProductID,
        SUM(it.Quantity) AS Total_Purchased
    FROM inventory_transactions it
    WHERE YEAR(it.Date) = 2023
      AND it.TransactionType = 'Purchase'
    GROUP BY it.ProductID
)

-- 3️⃣ النتيجة النهائية
SELECT
    p.ProductID,
    pm.ProductName,
    COALESCE(s.Total_Sold, 0) AS Total_Sold,
    COALESCE(p.Total_Purchased, 0) AS Total_Purchased,
    ROUND(
        COALESCE(s.Total_Sold, 0) / NULLIF(p.Total_Purchased, 0) * 100,
        2
    ) AS Sell_Through_Rate_Percent
FROM purchases_summary p
LEFT JOIN sales_summary s
    ON p.ProductID = s.ProductID
JOIN products_master pm
    ON p.ProductID = pm.ProductID;
    SELECT * FROM SellThrough_Rate;
    
    -- 3. Days of Inventory (DOI) / Days Inventory Outstanding (DIO)
CREATE OR REPLACE VIEW product_dio_clean AS
WITH
-- 1. حساب تكلفة البضاعة المباعة لكل منتج (COGS)
sales_with_cost AS (
    SELECT 
        s.ProductID,
        s.Quantity,
        (
            SELECT it.UnitCost
            FROM inventory_transactions it
            WHERE it.ProductID = s.ProductID
              AND it.TransactionType = 'Purchase'
              AND it.Date <= s.Date
            ORDER BY it.Date DESC
            LIMIT 1
        ) AS UnitCost
    FROM sales s
    WHERE YEAR(s.Date) = 2023
),
cogs_per_product AS (
    SELECT 
        ProductID,
        SUM(Quantity * UnitCost) AS COGS
    FROM sales_with_cost
    WHERE UnitCost IS NOT NULL
    GROUP BY ProductID
),
-- 2. متوسط قيمة المخزون لكل منتج
avg_inventory_value AS (
    SELECT 
        it.ProductID,
        AVG(it.StockLevelAfter * it.UnitCost) AS AvgInventoryValue
    FROM inventory_transactions it
    WHERE YEAR(it.Date) = 2023
      AND it.UnitCost IS NOT NULL
      AND it.StockLevelAfter IS NOT NULL
    GROUP BY it.ProductID
)
-- 3. حساب الـ DIO لكل منتج
SELECT 
    p.ProductID,
    ROUND(c.COGS, 2) AS COGS,
    ROUND(a.AvgInventoryValue, 2) AS AvgInventoryValue,
    ROUND((a.AvgInventoryValue / NULLIF(c.COGS, 0)) * 365, 1) AS DIO
FROM cogs_per_product c
JOIN avg_inventory_value a ON c.ProductID = a.ProductID
JOIN products_master p ON p.ProductID = a.ProductID
ORDER BY DIO DESC;


























    -- B.	Availability & Service KPIs (Stock Availability (Available to Sell - ATS, Stock Replenishment Efficiency ,
    -- Backorder Rate, Order Fulfillment Rate (Fill Rate)
    
    -- 1.	Stock Availability (Available to Sell - ATS)
create view stock_availability as
SELECT 
    it.ProductID,
    p.ProductName,
    SUM(
        CASE 
            WHEN it.TransactionType IN ('Purchase', 'Restock') THEN it.Quantity
            WHEN it.TransactionType IN ('Sale', 'ReturnToSupplier') THEN -it.Quantity
            ELSE 0
        END
    ) AS AvailableToSell
FROM inventory_transactions it
JOIN products_master p 
    ON it.ProductID = p.ProductID
GROUP BY it.ProductID, p.ProductName
ORDER BY p.ProductName;

-- 2.	Stock Replenishment Efficiency
create view stock_replenishment_efficiency as
SELECT 
    s.SupplierName,
    COUNT(*) AS TotalOrders,
    SUM(
        CASE 
            WHEN po.ActualDelivery <= po.ExpectedDelivery THEN 1
            ELSE 0
        END
    ) AS OnTimeDeliveries,
    ROUND(
        SUM(
            CASE 
                WHEN po.ActualDelivery <= po.ExpectedDelivery THEN 1
                ELSE 0
            END
        ) / COUNT(*) * 100, 2
    ) AS StockReplenishmentEfficiency
FROM purchase_orders po
JOIN suppliers s 
    ON po.SupplierID = s.SupplierID
GROUP BY s.SupplierName
ORDER BY StockReplenishmentEfficiency DESC;

-- 3.	Backorder Rate
create view backorder_rate as

SELECT 
    COUNT(*) AS TotalActiveOrders,
    SUM(
        CASE 
            WHEN Status IN ('Pending', 'Shipped') THEN 1
            ELSE 0
        END
    ) AS BackorderedOrders,
    ROUND(
        SUM(
            CASE 
                WHEN Status IN ('Pending', 'Shipped') THEN 1
                ELSE 0
            END
        ) / COUNT(*) * 100, 
        2
    ) AS BackorderRate
FROM purchase_orders
WHERE Status <> 'Cancelled';

-- 4.Order Fulfillment Rate (Fill Rate)
create view fillrate as
SELECT 
    COUNT(*) AS TotalOrders,
    SUM(CASE WHEN Status = 'Delivered' THEN 1 ELSE 0 END) AS FullyReceivedOrders,
    ROUND(
        SUM(CASE WHEN Status = 'Delivered' THEN 1 ELSE 0 END) / COUNT(*) * 100, 
        2
    ) AS PurchaseOrderFulfillmentRate
FROM purchase_orders
WHERE Status <> 'Cancelled';

-- C.	Accuracy & Forecasting KPIs(Inventory Accuracy, Forecast Accuracy (MAPE / MAPE%), Lead Time & Lead Time Variability)
-- 1.Inventory Accuracy
-- 1. آخر رصيد في النظام لكل منتج (System Stock)
CREATE OR REPLACE VIEW v_system_stock AS
SELECT
  it.ProductID,
  MAX(it.Date) AS last_transaction,
  (SELECT t.StockLevelAfter
   FROM inventory_transactions t
   WHERE t.ProductID = it.ProductID
   ORDER BY t.Date DESC, t.TransactionID DESC
   LIMIT 1) AS system_stock
FROM inventory_transactions it
GROUP BY it.ProductID;

-- 2. آخر جرد فعلي معروف (Physical Count)
CREATE OR REPLACE VIEW v_physical_count AS
SELECT
  it.ProductID,
  it.Date AS count_date,
  it.StockLevelAfter AS physical_count
FROM inventory_transactions it
WHERE it.TransactionType IN ('Cycle Count','Stocktake','Adjustment','Inventory Adjustment')
  AND it.StockLevelAfter IS NOT NULL
  AND it.Date = (
    SELECT MAX(t2.Date)
    FROM inventory_transactions t2
    WHERE t2.ProductID = it.ProductID
      AND t2.TransactionType IN ('Cycle Count','Stocktake','Adjustment','Inventory Adjustment')
  );
  
  CREATE OR REPLACE VIEW v_inventory_accuracy AS
SELECT
  s.ProductID,
  s.system_stock,
  p.physical_count,
  CASE
    WHEN p.physical_count IS NULL THEN NULL
    WHEN p.physical_count = 0 THEN 0
    ELSE ROUND((1 - ABS(s.system_stock - p.physical_count)/p.physical_count) * 100, 2)
  END AS inventory_accuracy_pct
FROM v_system_stock s
LEFT JOIN v_physical_count p ON p.ProductID = s.ProductID;
SELECT * FROM v_inventory_accuracy ORDER BY inventory_accuracy_pct desc LIMIT 10;

-- Forecast Accuracy (MAPE / MAPE%)
CREATE OR REPLACE VIEW v_monthly_sales AS
SELECT
  ProductID,
  DATE_FORMAT(Date, '%Y-%m-01') AS sale_month,
  SUM(Quantity) AS qty_sold
FROM sales
GROUP BY ProductID, DATE_FORMAT(Date, '%Y-%m-01');

CREATE OR REPLACE VIEW v_monthly_forecast AS
SELECT
  ProductID,
  sale_month,
  qty_sold,
  ROUND(AVG(qty_sold) OVER (
      PARTITION BY ProductID
      ORDER BY sale_month
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ),2) AS forecast_qty
FROM v_monthly_sales;

SELECT * FROM v_forecast_accuracy ORDER BY MAPE_pct desc LIMIT 10;

-- 3.Lead Time & Lead Time Variability 
CREATE OR REPLACE VIEW v_leadtime AS
SELECT
  PO_ID,
  ProductID,
  SupplierID,
  DATEDIFF(ActualDelivery, OrderDate) AS lead_time_days
FROM purchase_orders
WHERE ActualDelivery IS NOT NULL AND OrderDate IS NOT NULL;

SELECT
  ROUND(AVG(lead_time_days),2) AS avg_lead_time_days,
  ROUND(MIN(lead_time_days),2) AS min_lead_time,
  ROUND(MAX(lead_time_days),2) AS max_lead_time
FROM v_leadtime;

CREATE OR REPLACE VIEW v_leadtime_variability AS
SELECT
  SupplierID,
  COUNT(*) AS deliveries,
  ROUND(AVG(lead_time_days),2) AS avg_leadtime,
  ROUND(STDDEV_POP(lead_time_days),2) AS stddev_leadtime,
  CASE
    WHEN AVG(lead_time_days)=0 THEN NULL
    ELSE ROUND(STDDEV_POP(lead_time_days)/AVG(lead_time_days),3)
  END AS variability_ratio
FROM v_leadtime
GROUP BY SupplierID;
SELECT * FROM v_leadtime_variability ORDER BY variability_ratio DESC LIMIT 10;

-- Financial KPIs ( Gross Margin Return on Inventory (GMROI),Carrying Cost % of Inventory,Obsolete / Dead Stock )
-- 1-Gross Margin Return on Inventory (GMROI):  
-- حساب GMROI لكل منتج
create view GMROI as
SELECT 
    p.ProductName,
    
    -- إجمالي الإيرادات
    SUM(s.Quantity * s.SellingPrice) AS SalesRevenue,
    
    -- تكلفة البضاعة المباعة
    SUM(s.Quantity * p.UnitCost) AS COGS,
    
    -- متوسط المخزون (من البيانات الفعلية)
    AVG(it.StockLevelAfter * p.UnitCost) AS AverageInventoryValue,
    
    -- حساب Gross Margin و GMROI
    ROUND(SUM(s.Quantity * s.SellingPrice) - SUM(s.Quantity * p.UnitCost), 2) AS GrossMargin,
    ROUND(
        (SUM(s.Quantity * s.SellingPrice) - SUM(s.Quantity * p.UnitCost)) /
        NULLIF(AVG(it.StockLevelAfter * p.UnitCost), 0), 
        2
    ) AS GMROI

FROM sales s
JOIN products_master p 
    ON s.ProductID = p.ProductID
JOIN inventory_transactions it 
    ON s.ProductID = it.ProductID

GROUP BY p.ProductName
ORDER BY GMROI DESC;

-- 2-Carrying Cost % of Inventory
create view Carrying_Cost_of_Inventory as 
SELECT
    p.ProductName,
    AVG(it.StockLevelAfter * p.UnitCost) AS AverageInventoryValue,
    
    -- تقدير التكلفة السنوية للاحتفاظ بالمخزون (25%)
    ROUND(AVG(it.StockLevelAfter * p.UnitCost) * 0.25, 2) AS AnnualCarryingCost,
    
    -- النسبة المئوية
    ROUND(
        (AVG(it.StockLevelAfter * p.UnitCost) * 0.25) /
        NULLIF(AVG(it.StockLevelAfter * p.UnitCost), 0) * 100, 
        2
    ) AS CarryingCostPercent

FROM products_master p
JOIN inventory_transactions it 
    ON p.ProductID = it.ProductID

GROUP BY p.ProductName
ORDER BY CarryingCostPercent DESC;

-- 3-Obsolete / Dead Stock :
  
CREATE TEMPORARY TABLE latest_tx2 AS
SELECT ProductID, LastTransactionDate, StockLevelAfter
FROM (
  SELECT 
    it.ProductID,
    it.Date AS LastTransactionDate,
    it.StockLevelAfter,
    ROW_NUMBER() OVER (
      PARTITION BY it.ProductID 
      ORDER BY it.Date DESC, COALESCE(it.TransactionID, '') DESC
    ) AS rn
  FROM inventory_transactions it
) t
WHERE rn = 1;
CREATE TEMPORARY TABLE product_movement5 AS
SELECT
  p.ProductID,
  p.ProductName,
  lt.LastTransactionDate,
  DATEDIFF('2023-12-31', lt.LastTransactionDate) AS DaysSinceLastTransaction,
  COALESCE(lt.StockLevelAfter, 0) AS StockLevel,
  ROUND(COALESCE(lt.StockLevelAfter, 0) * COALESCE(p.UnitCost, 0), 2) AS InventoryValue,
  CASE
    WHEN lt.LastTransactionDate IS NULL THEN 'No Transactions'
    WHEN DATEDIFF('2023-12-31', lt.LastTransactionDate) <= 90 THEN 'Fast-moving'
    WHEN DATEDIFF('2023-12-31', lt.LastTransactionDate) BETWEEN 91 AND 180 THEN 'Slow-moving'
    ELSE 'Dead Stock'
  END AS MovementCategory
FROM products_master p
LEFT JOIN latest_tx lt ON p.ProductID = lt.ProductID;
SELECT 
  MovementCategory,
  COUNT(*) AS NumProducts,
  SUM(StockLevel) AS TotalUnitsInCategory,
  ROUND(SUM(InventoryValue), 2) AS TotalValueInCategory
FROM product_movement5
GROUP BY MovementCategory;
SELECT
  ROUND(SUM(CASE WHEN MovementCategory = 'Dead Stock' THEN InventoryValue ELSE 0 END), 2) AS DeadStockValue,
  ROUND(SUM(InventoryValue), 2) AS TotalInventoryValue,
  CASE 
    WHEN SUM(InventoryValue) = 0 THEN NULL
    ELSE ROUND(
      SUM(CASE WHEN MovementCategory = 'Dead Stock' THEN InventoryValue ELSE 0 END)
      / SUM(InventoryValue) * 100, 2)
  END AS DeadStockPercent
FROM product_movement5;

-- D.Operational KPIs  (Order Cycle Time,Perfect Order Rate)
CREATE OR REPLACE VIEW kpi_operational AS
SELECT 
    ROUND(AVG(DATEDIFF(po.ActualDelivery, po.OrderDate)), 2) AS AvgOrderCycleTime_Days,
    ROUND(SUM(CASE WHEN po.ActualDelivery <= po.ExpectedDelivery THEN 1 ELSE 0 END) 
          / COUNT(*) * 100, 2) AS PerfectOrderRate_Percent,
    (SELECT ROUND(SUM(CASE WHEN it.StockLevelAfter < 0 THEN 1 ELSE 0 END) 
                  / COUNT(*) * 100, 2)
     FROM inventory_transactions it
     WHERE it.TransactionType = 'Sale') AS BackorderRate_Percent
FROM purchase_orders po
WHERE po.Status = 'Delivered' AND po.ActualDelivery IS NOT NULL;











    