--- Task 1
-- 1. Визначення попереднього замовлення для кожного клієнта
SELECT
    CustomerID, OrderID, OrderDate,
    LAG(OrderID) OVER(PARTITION BY CustomerID ORDER BY OrderDate) AS Previous_Order_ID
FROM Orders

--- Task 2
-- 2. Перше замовлення кожного клієнта**

SELECT
    CustomerID, OrderID, OrderDate,
    FIRST_VALUE(OrderID) OVER(PARTITION BY CustomerID ORDER BY OrderDate) AS First_Order_ID1,
    MIN(OrderID) OVER(PARTITION BY CustomerID ORDER BY OrderDate) AS First_Order_ID2
FROM Orders

--- Task 3
-- 3. ТОП-3 найдорожчі замовлення у кожній категорії товарів
SELECT *
FROM (SELECT c.CategoryID,
             c.CategoryName,
             p.ProductID,
             p.ProductName,
             p.UnitPrice,
             RANK() OVER (PARTITION BY c.CategoryID ORDER BY p.UnitPrice DESC) AS UnitPrice_Rank
      FROM Products p
               JOIN Categories C
                    ON c.CategoryID = p.CategoryID
      )t WHERE UnitPrice_Rank <= 3

--- Task 4
-- 4. З яким проміжком часу користувачі роблять замовлення.

SELECT
    CustomerID, OrderID, OrderDate AS CurrentDate,
    LAG(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate) AS Previous_Order_Date,
    DATEDIFF(day, LAG(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate), OrderDate) AS Diff_Days
FROM Orders

--- Task 5
-- Середній інтервал в днях між датами замовлень клієнтів по регіонам.
SELECT COALESCE(Region, 'Unknown') AS Region,
       ROUND(AVG(Diff_Days), 2) AS Avg_Diff_Days
FROM (SELECT o.CustomerID,
             o.OrderID,
             c.Region,
             o.OrderDate                                                                                        AS CurrentDate,
             LAG(o.OrderDate) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate)                             AS Previous_Order_Date,
             DATEDIFF(day, LAG(o.OrderDate) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate),
                      o.OrderDate)                                                                              AS Diff_Days

      FROM Orders o
               JOIN Customers c
                    ON c.CustomerID = o.CustomerID
      )t WHERE Diff_Days is not null
GROUP BY Region

-- a) In ID region Customers make purchases the most often
-- The least often region for purchases is DF

-- b) Population, Number of clients per Region, Number of Total Orders, the time period of data,

--- Task 6
-- ТОП-3 співробітники за кількістю оброблених замовлень
SELECT *
FROM (SELECT e.EmployeeID,
             e.FirstName,
             e.LastName,
             COUNT(o.OrderID)                             AS Order_Count,
             RANK() OVER (ORDER BY COUNT(o.OrderID) DESC) AS Employee_Rank
      FROM Employees e
               JOIN Orders o
                    ON e.EmployeeID = o.EmployeeID
      GROUP BY e.EmployeeID, e.FirstName, e.LastName
      )t WHERE Employee_Rank <= 3


--- Task 7
-- ТОП-3 співробітники за кількістю оброблених замовлень у кожному регіоні

SELECT *
FROM (SELECT c.Region,
             e.EmployeeID,
             e.FirstName,
             e.LastName,
             COUNT(o.OrderID)                             AS Order_Count,
             ROW_NUMBER() OVER(PARTITION BY c.Region ORDER BY COUNT(o.OrderID) DESC) AS Employee_Rank
      FROM Employees e
               JOIN Orders o
                    ON e.EmployeeID = o.EmployeeID
               JOIN Customers c
                    ON o.CustomerID = c.CustomerID
      GROUP BY c.Region, e.EmployeeID, e.FirstName, e.LastName
      )t WHERE Employee_Rank <= 3
ORDER BY Region