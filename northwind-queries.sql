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