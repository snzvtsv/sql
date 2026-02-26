-- check whether the table 'orders' contains any duplicate rows

SELECT
*
FROM (
  SELECT
	o.order_id,
    COUNT(1) OVER (PARTITION BY o.order_id) CheckDR
FROM orders o
)t where CheckDR > 1


 -- Find the total sales across all orders and the total sales for each product.
 -- Additionally, provide details such as order ID and order date

SELECT
    od.OrderID,
    p.ProductName,
    o.OrderDate,
    od.quantity AS Sales,
    s.order_status,
    SUM(od.quantity) OVER () AS TotalSales,
    SUM(od.quantity) OVER (PARTITION BY od.ProductID) AS TotalSalesByProducts
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID
JOIN (
    SELECT
        OrderID,
        CASE
            WHEN ShippedDate IS NULL THEN 'Not Shipped'
            WHEN ShippedDate > RequiredDate THEN 'Delivered Late'
            ELSE 'Delivered On Time'
        END AS order_status
    FROM orders
) s
    ON od.OrderID = s.OrderID;


-- Find the % contribution of each product's sales to the total sales

-- Perc Contr = (Prod's sales / Total Sales) * 100
SELECT
    od.OrderID,
    p.ProductName,
    od.quantity AS Sales,
    SUM(od.quantity) OVER () AS TotalSales,
    SUM(od.quantity) OVER (PARTITION BY od.ProductID) AS TotalSalesByProducts,
    ROUND (( CAST (od.Quantity AS FLOAT) / SUM(od.quantity) OVER () ) * 100, 2) AS PerContributionByProd

FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

-- JOIN (
--     SELECT
--         OrderID,
--         CASE
--             WHEN ShippedDate IS NULL THEN 'Not Shipped'
--             WHEN ShippedDate > RequiredDate THEN 'Delivered Late'
--             ELSE 'Delivered On Time'
--         END AS order_status
--     FROM orders
-- ) s
--     ON od.OrderID = s.OrderID;

-- Find the average sales across all orders and the average sales for each product
-- Additionally, provide details such OrderID, OrderDate

SELECT
    od.OrderID,
    p.ProductName,
    od.quantity AS Sales,
    SUM(od.quantity) OVER () AS TotalSales,
    --SUM(od.quantity) OVER (PARTITION BY od.ProductID) AS TotalSalesByProducts,
    AVG(od.quantity) OVER (PARTITION BY ProductName) AS AVGProd,
    AVG(od.quantity) OVER () AS AVGTotalSales

FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

-- Find the average score of customers, Show CustomerID, Last Name
SELECT c.*,
    od.quantity as Sales,
    RIGHT(ContactName, CHARINDEX(' ', REVERSE(ContactName))) AS LastName,
    AVG(od.quantity) OVER() AS AvgSales,

FROM Customers C
JOIN Orders o
    ON c.CustomerID = o.CustomerID
JOIN [Order Details] od
    ON o.OrderID = od.OrderID

SELECT od.quantity,
       COUNT(*) OVER (PARTITION BY od.OrderID) AS CheckFDup
FROM [Order Details] od

SELECT
    COUNT(od.quantity) AS NullCount
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

SELECT
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS NullCount
From [Order Details]


-- Find all orders where sales are higher than the average sales across all orders

SELECT
    od.OrderID,
    p.ProductName,
    od.quantity AS Sales,
    --SUM(od.quantity) OVER () AS TotalSales,
    AVG(od.quantity) OVER () AS AVGTotalSales
    --SUM(od.quantity) OVER (PARTITION BY od.ProductID) AS TotalSalesByProducts,
    --AVG(od.quantity) OVER (PARTITION BY ProductName) AS AVGProd
    --CASE WHEN (od.quantity > AVG(od.quantity) OVER () THEN  )
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID


SELECT *
FROM(
    SELECT od.OrderID,
             p.ProductName,
             od.quantity              AS Sales,
             --SUM(od.quantity) OVER () AS TotalSales,
             AVG(od.quantity) OVER () AS AVGTotalSales
      --SUM(od.quantity) OVER (PARTITION BY od.ProductID) AS TotalSalesByProducts,
      --AVG(od.quantity) OVER (PARTITION BY ProductName) AS AVGProd
      --CASE WHEN (od.quantity > AVG(od.quantity) OVER () THEN  )
      FROM [Order Details] od
               JOIN products p
                    ON od.ProductID = p.ProductID
               JOIN Orders o
                    ON od.OrderID = o.OrderID
      )t WHERE Sales > AVGTotalSales

-- Find the highest&lowest sales across all orders
-- And the highest & lowest sales for each product
-- Additionally, provide details such as order ID and order date


SELECT
    od.OrderID,
    o.OrderDate,
    p.ProductID,
    od.quantity AS Sales,

    MAX(od.quantity) OVER () AS MaxSales,
    MIN(od.quantity) OVER () AS MinSales,

    --MAX(od.quantity) OVER (Partition BY p.ProductName) AS MaxSalesProd,
    --MIN(od.quantity) OVER (PARTITION BY p.ProductName) AS MinSalesProd,

    MAX(od.quantity) OVER (Partition BY p.ProductID) AS MaxSalesProd,
    MIN(od.quantity) OVER (PARTITION BY p.ProductID) AS MinSalesProd
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

-- Show the employees who have the highest salaries

WITH Base AS (
        SELECT
            o.OrderID, o.EmployeeID, od.ProductID, od.UnitPrice, od.Quantity, od.Discount,
            o.ShippedDate, o.RequiredDate,

            -- REVENUE
            od.UnitPrice * od.Quantity * (1 - od.Discount) AS Revenue,

            -- COST
            od.UnitPrice * 0.7 * od.Quantity AS Cost,

            -- Profit
            (od.UnitPrice * od.Quantity * (1 - od.Discount) * 0.3) AS Profit,

            -- Volume Coefficient
            CASE
                WHEN od.quantity > 80 THEN 0.8 ELSE 1
            END AS VolumeCoeff,

            -- DeliveryCoefficient
           CASE
                WHEN o.ShippedDate IS NULL THEN 0 -- Unshipped
                WHEN o.ShippedDate > o.RequiredDate THEN 0.7 -- Delivered late
                ELSE 1 --'Delivered On Time'
            END AS DeliveryCoeff

        FROM Employees e
        JOIN Orders o
            ON e.EmployeeID = o.EmployeeID
        JOIN [Order Details] od
            ON o.OrderID = od.OrderID

    ),

    SalaryPerEmployee AS (
    SELECT
        b.EmployeeID,
        ROUND(SUM(0.05 * Profit * VolumeCoeff * DeliveryCoeff), 2) AS Salary
    FROM Base b
    GROUP BY b.EmployeeID
)

SELECT
    spe.EmployeeID,
    spe.Salary,
    MAX(Salary) OVER() AS HighestSalary,
    MIN(Salary) OVER() AS LowestSalary
FROM SalaryPerEmployee spe
ORDER BY EmployeeID



-- SELECT
--     b.EmployeeID,
--     ROUND(SUM(b.Revenue), 2)AS TotalRev,
--     ROUND(SUM(b.Cost), 2) AS TotalCost,
--     ROUND(SUM(b.Revenue - b.Cost), 2) AS TotalProfit,
--     ROUND(SUM(0.05 * (b.Revenue - b.Cost) * b.VolumeCoeff * b.DeliveryCoeff), 2) AS TotalCommissionKPI,
--     ROUND(SUM(0.05 * Profit * VolumeCoeff * DeliveryCoeff), 2) AS Salary,
--     MAX(SUM(0.05 * Profit * VolumeCoeff * DeliveryCoeff)) AS HighestSalary,
--     MIN(SUM(0.05 * Profit * VolumeCoeff * DeliveryCoeff)) AS LowSalary
-- FROM Base b
-- GROUP BY b.EmployeeID
-- ORDER BY b.EmployeeID

--     b.OrderID,
--     b.EmployeeID,
--     b.Revenue, b.Cost,
--     b.Revenue - b.Cost AS Profit,
--     0.05 * (b.Revenue - b.Cost) * b.VolumeCoeff * b.DeliveryCoeff AS CommissionKPI,
--     SUM(0.05 * Profit * VolumeCoeff * DeliveryCoeff) OVER(PARTITION BY b.EmployeeID) AS Salary

-- Find the deviation of each sales from the minimum and maximum sales amounts

SELECT
    od.OrderID,
    p.ProductName,
    od.quantity AS Sales,
    MAX(od.quantity) OVER() AS HighestSales,
    MIN(od.quantity) OVER() AS LowestSales,
    od.quantity - MIN(od.quantity) OVER() AS DeviationFromMin,
    MAX(od.quantity) OVER() - od.quantity AS DeviationFromMax

FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID


-- Calculate the moving average (running average) of sales for each product over time
SELECT
    od.OrderID,
    od.ProductID,
    o.OrderDate,
    od.quantity AS Sales,
    AVG(od.quantity) OVER(PARTITION BY od.ProductID) AS AvgByProduct,
    AVG(od.quantity) OVER(PARTITION BY od.ProductID ORDER BY OrderDate) AS MovingAvg
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

-- Calculate the moving average of sales for each product over time, including only the next order

SELECT
    od.OrderID,
    od.ProductID,
    o.OrderDate,
    od.quantity AS Sales,
    AVG(od.quantity) OVER(PARTITION BY od.ProductID) AS AvgByProduct,
    AVG(od.quantity) OVER(PARTITION BY od.ProductID ORDER BY OrderDate) AS MovingAvg,
    AVG(od.quantity) OVER(PARTITION BY od.ProductID ORDER BY OrderDate ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS RollingAvg
FROM [Order Details] od
JOIN products p
    ON od.ProductID = p.ProductID
JOIN Orders o
    ON od.OrderID = o.OrderID

-- Rank the orders based on their sales from the highest to the lowest
SELECT
    OrderID,
    ProductID,
    Quantity AS Sales,
    ROW_NUMBER() OVER(ORDER BY Quantity DESC) AS SalesRank_Row,
    RANK() OVER(ORDER BY Quantity DESC) AS SalesRank_Rank,
    DENSE_RANK() OVER(ORDER BY Quantity DESC) AS SalesDense_Rank
FROM [Order Details]

-- ROW_NUMBER() TOP-N Analysis
-- Find the top highest sales for each product
SELECT *
FROM (SELECT OrderID,
             ProductID,
             Quantity AS Sales,
             ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY quantity DESC) AS RankByProduct
      FROM [Order Details]
      )t WHERE RankByProduct = 1

-- ROW_NUMBER() BOTTOM-N Analysis
-- Find the lowest 2 customers based on their total sales
SELECT *
FROM (
    SELECT c.CustomerID,
             SUM(od.Quantity)                                  AS TotalSales,
             ROW_NUMBER() OVER (ORDER BY SUM(od.Quantity) ASC) AS RankCustomers
      FROM Customers c
               JOIN Orders o
                    ON c.CustomerID = o.CustomerID
               JOIN [Order Details] od
                    ON o.OrderID = od.OrderID
      GROUP BY c.CustomerID)t WHERE RankCustomers <= 2


-- Assign unique IDs to the rows of the 'Order Archive' table
-- Divide(partition) the data by the primary key of the table
 -- Latest inserted data
SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY o.OrderID ORDER BY o.OrderDate DESC) AS RN,
                      o.*
               FROM [Order Details] od
                        JOIN Orders o
                             ON od.OrderID = o.OrderID
                        JOIN (SELECT OrderID,
                                     CASE
                                         WHEN ShippedDate IS NULL THEN 'Not Shipped'
                                         WHEN ShippedDate > RequiredDate THEN 'Delivered Late'
                                         ELSE 'Delivered On Time'
                                         END AS order_status
                              FROM orders) s
                             ON od.OrderID = s.OrderID)t WHERE RN=1

--Uncleaned/Problematic data
SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY o.OrderID ORDER BY o.OrderDate DESC) AS RN,
                      o.*
               FROM [Order Details] od
                        JOIN Orders o
                             ON od.OrderID = o.OrderID
                        JOIN (SELECT OrderID,
                                     CASE
                                         WHEN ShippedDate IS NULL THEN 'Not Shipped'
                                         WHEN ShippedDate > RequiredDate THEN 'Delivered Late'
                                         ELSE 'Delivered On Time'
                                         END AS order_status
                              FROM orders) s
                             ON od.OrderID = s.OrderID)t WHERE RN>1

SELECT
    OrderID,
    Quantity AS Sales,
    NTILE(1) OVER(ORDER BY Quantity DESC) OneBuckets,
    NTILE(2) OVER(ORDER BY Quantity DESC) TwoBuckets,
    NTILE(3) OVER(ORDER BY Quantity DESC) ThreeBuckets,
    NTILE(150) OVER(ORDER BY Quantity DESC) Buckets
FROM [Order Details]

-- Segment all orders into 3 categories: high, medium and low sales
SELECT
*,
CASE WHEN Buckets = 1 THEN 'High'
    WHEN Buckets = 2 THEN 'Medium'
    WHEN Buckets = 3 THEN 'Low'
END AS SalesSegmentation
FROM (
    SELECT OrderID,
             Quantity                               AS Sales,
             NTILE(3) OVER (ORDER BY Quantity DESC) AS Buckets

      FROM [Order Details]
      )t

-- IN order to export the data, divide the orders into 20 groups
SELECT
    NTILE(20) OVER(ORDER BY OrderID) AS Buckets,
    *
FROM [Order Details]

-- Find the products that fall within the highest 40% of the prices
SELECT *,
       CONCAT(DistRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        ProductID, ProductName, UnitPrice,
        ROUND(CUME_DIST() OVER(ORDER BY UnitPrice ASC), 3) AS DistRank
    FROM Products p
     )t WHERE DistRank >= 0.6

SELECT *,
       CONCAT(PercRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        ProductID, ProductName,
        ROUND(Percent_Rank() OVER(ORDER BY UnitPrice DESC), 3) AS PercRank
    FROM Products p
     )t WHERE PercRank <= 0.4

