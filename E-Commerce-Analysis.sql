-- 14
with customer_stats as (
    select 
        CustomerID,
        count(*) as order_count,
        sum(cast(`Sale Price` as DECIMAL(10,2))) as total_revenue,
        avg(`Sale Price`) as avg_order_value
    from Orderss
    group by CustomerID
),
normalized as (
    select 
        CustomerID,
        total_revenue,
        order_count,
        avg_order_value,
        (total_revenue - min(total_revenue) over ()) / nullif((max(total_revenue) over () - min(total_revenue) over ()), 0) as norm_revenue,
        (order_count - min(order_count) over ()) / nullif((max(order_count) over() - min(order_count) over()), 0) as norm_orders,
        (avg_order_value - min(avg_order_value) over ()) / nullif((max(avg_order_value) over () - MIN(avg_order_value) over ()), 0) as norm_avg_order
    from customer_stats
),
composite_score as (
    select 
        CustomerID,
        total_revenue,
        order_count,
        avg_order_value,
        round((norm_revenue * 0.5 + norm_orders * 0.3 + norm_avg_order * 0.2), 4) as score
    from normalized
)
select *
from composite_score
order by score desc
limit 5;


-- 15
with Total_Saless as(select DATE_FORMAT(`OrderDate`, '%Y-%m') as month_year,
sum(cast(`Sale Price` as decimal(10,2))) as summ
from Orderss
group by month_year
order by month_year)
select month_year, summ as Total_Sales, 
round(100 * (summ-lag(summ) over (order by month_year))/(lag(summ) over (order by month_year)),2) as perc_change
from Total_Saless;

 
-- 16
with Rolling_avg as ( select product_category, date_format(OrderDate,"%Y-%m") as month_year,
 sum(cast(`Sale Price` as decimal(10,2))) as summ
 from orderss
 group by product_category, month_year
 order by product_category,month_year
)
  select product_category, month_year, round(avg(summ) over (partition by product_category order by month_year 
 rows between 3 preceding and current row),2) as rolling_avg
 from Rolling_avg;
 
 
 -- 17
update Orderss
set cast(`Sale Price` as decimal(10,2)) = cast(`Sale Price` as decimal(10,2)) * 0.85
where CustomerID in (
    select CustomerID
    from (
        select CustomerID
        from Orderss
        group by CustomerID
        having count(*) >= 10
    ) as eligible_customers
);


-- 18
with RankedOrders as (
  select
    CustomerID,
    OrderDate,
    row_number() over (partition by CustomerID order by OrderDate) as rn
   from Orderss
),
OrdersWithPrev as (
  select
    ro.CustomerID,
    ro.OrderDate,
    datediff(ro.OrderDate, ro_prev.OrderDate) as days_diff
  from RankedOrders ro
  join RankedOrders ro_prev 
    on ro.CustomerID = ro_prev.CustomerID 
    and ro.rn = ro_prev.rn + 1
),
CustomersWith5Orders as (
  select CustomerID
  from Orderss
  group by CustomerID
  having count(*) >= 5
)
select
  avg(days_diff) as avg_days_between_orders
from OrdersWithPrev
where CustomerID in (select CustomerID from CustomersWith5Orders);

-- 19
with CustomerRevenue as (
  select
    CustomerID,
    sum(cast(`Sale Price` as decimal(10,2))) as total_revenue
  from Orderss
  group by CustomerID
),
AverageRevenue as(
  select
    avg(total_revenue) as avg_revenue
  from CustomerRevenue
)
select
  cr.CustomerID,
  cr.total_revenue
from CustomerRevenue cr
join AverageRevenue ar
  on cr.total_revenue > ar.avg_revenue * 1.3;

-- 20
with YearlySales as(
  select
    `Product_Category`,
    date_format(OrderDate, "%YYYY") as sales_year,
    sum(CAST(`Sale Price` as decimal(10,2))) as total_sales
  from Orderss
  group by `Product_Category`, date_format(OrderDate, "%YYYY")
),
SalesComparison as(
  select
    curr.`Product_Category`,
    curr.total_sales as current_year_sales,
    prev.total_sales as previous_year_sales,
    (curr.total_sales - prev.total_sales) as sales_difference,
    round(((curr.total_sales - prev.total_sales) / nullif(prev.total_sales, 0)) * 100, 2) as percent_growth
  from YearlySales curr
  join YearlySales prev
    on curr.`Product_Category` = prev.`Product_Category`
   and curr.sales_year = prev.sales_year + 1
)
select
  `Product_Category`,
  current_year_sales,
  previous_year_sales,
  percent_growth
from SalesComparison
order by percent_growth desc
limit 3;


 
