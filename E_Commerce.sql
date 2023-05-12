--Using the columns of “market_fact”, “cust_dimen”, “orders_dimen”,“prod_dimen”, “shipping_dimen”, Create a new table, named as“combined_table”.

select A.Cust_ID,Customer_Name,Order_ID,Province,Region,Customer_Segment,B.Prod_ID,B.Ship_ID,Sales,Discount,Order_Quantity,Product_Base_Margin,
C.Order_Date,Order_Priority,Product_Category,Product_Sub_Category,Ship_Mode,Ship_Date
into
combinedtable
from [dbo].[cust_dimen] A,[dbo].[market_fact] B,[dbo].[orders_dimen] C,[dbo].[prod_dimen] D,[dbo].[shipping_dimen] E
where A.Cust_ID = B.Cust_ID
and B.Ord_ID =C.Ord_ID
and B.Prod_ID = D.Prod_ID
and B.Ship_ID = E.Ship_ID

----. Find the top 3 customers who have the maximum count of orders.
select top(3) Cust_ID,Customer_Name,sum(Order_Quantity) number_of_orders
from combinedtable
group by Cust_ID,Customer_Name
order by 3 desc
---or-------
select distinct Customer_Name,number_of_orders
from 
	(select top(3) Cust_ID,count(distinct Order_ID) number_of_orders
	from combinedtable
	group by Cust_ID
	order by 2 desc) A ,combinedtable
where A.Cust_ID = combinedtable.Cust_ID
order by 2 desc
--. Create a new column at combined_table as DaysTakenForShipping that contains the date difference of Order_Date and Ship_Date.
alter table combinedtable 
		add DaysTakenForShipping int

update combinedtable
		set DaysTakenForShipping = datediff(Order_Date,Ship_Date)
-- Find the customer whose order took the maximum time to get shipping

select distinct Customer_Name,DaysTakenForShipping
from combinedtable
where DaysTakenForShipping =
		(select max(DaysTakenForShipping)
		from combinedtable)

----Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
--1
select count(distinct Cust_ID) total_number_of_unique_customers_in_January 
from combinedtable
where month(Order_Date) = 01
and year(Order_Date) = 2011
--2 
select *
from
(select  distinct Cust_ID, month(order_date) [month]
from combinedtable
where Cust_ID in
		(select distinct Cust_ID
		from combinedtable
		where month(Order_Date) = 01
		and year(Order_Date) = 2011)) A
pivot
	(count(Cust_ID) for [month]  in ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])) pvt_table

--Write a query to return for each user the time elapsed between the first purchasing and the third purchasing, in ascending order by Customer ID.


with t1 as
(
select distinct Cust_ID,Order_Date,min(Order_Date) over (partition by Cust_ID) first_order,dense_rank() over (partition by Cust_ID  order by Order_Date) rank_order
from combinedtable
)
select Cust_ID,datediff(day,first_order,Order_Date) elapsed_day
from t1
where rank_order = 3
order by Cust_ID 
--Write a query that returns customers who purchased both product 11 and
--product 14, as well as the ratio of these products to the total number of
--products purchased by the customer.

with t2 as
(
select Cust_ID,Prod_ID,Order_Quantity,
case when Prod_ID = 'Prod_11' then 1*Order_Quantity else 0 end as purc_Prod_11,
case when Prod_ID = 'Prod_14' then 1*Order_Quantity else 0 end as purc_Prod_14,
sum(Order_Quantity) over (partition by Cust_ID) total_Prod
from 
combinedtable
where Cust_ID in (select distinct Cust_ID
				from combinedtable
				where Cust_ID in (select distinct Cust_ID
									from combinedtable
								where Prod_ID = 'Prod_11')
				and Prod_ID = 'Prod_14')
)
select distinct Cust_ID,sum(purc_Prod_11) over (partition by Cust_ID) total_11,(sum(purc_Prod_11) over (partition by Cust_ID))*1.0/total_Prod as ratio_11,
						sum(purc_Prod_14) over (partition by Cust_ID) total_14,(sum(purc_Prod_14) over (partition by Cust_ID))*1.0/total_Prod as ratio_14,
						total_Prod
from t2

---------PART TWO----------------------
--Create a “view” that keeps visit logs of customers on a monthly basis. (For each log, three field is kept: Cust_id, Year, Month)

Create view cust_logs as
select distinct Cust_ID,year(Order_Date) [year],month(Order_Date) [month]
from combinedtable
--Create a “view” that keeps the number of monthly visits by users. (Show separately all months from the beginning business)
create view num_ord_bymonths as
select distinct Cust_ID,Order_ID,Order_Date,year(Order_Date) [year],month(Order_Date) month_num,count(Order_ID) over (partition by Cust_ID,year(Order_Date),month(Order_Date)) num_orders
from combinedtable

--For each visit of customers, create the next month of the visit as a separate column.

select *,
lead(Order_date) over(partition by Cust_ID order by Order_Date) next_visit,
year(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_year,
month(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_month
from num_ord_bymonths
order by Cust_ID,[year],month_num

---Calculate the monthly time gap between two consecutive visits by each customer.

with t3 as
(
select *,
lead(Order_date) over(partition by Cust_ID order by Order_Date) next_visit,
year(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_year,
month(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_month
from num_ord_bymonths

)
select Cust_ID,datediff(month,Order_Date,next_visit) time_gap
from t3
order by Cust_ID

-- Categorise customers using average time gaps. Choose the most fitted labeling model for you.

--For example:
--o Labeled as churn if the customer hasn't made another purchase in the months since they made their first purchase.
--o Labeled as regular if the customer has made a purchase every month.
create view time_gaps as
select *,datediff(month,Order_Date,lead(Order_date) over(partition by Cust_ID order by Order_Date)) time_gap
from (select *,
lead(Order_date) over(partition by Cust_ID order by Order_Date) next_visit,
year(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_year,
month(lead(Order_date) over(partition by Cust_ID order by Order_Date)) next_visit_month
from num_ord_bymonths) A

with t5 as
(
select distinct Cust_ID,
avg(time_gap) over (partition by Cust_ID) avg_time_gaps,
datediff(month,(last_value(Order_Date) over (partition by Cust_ID order by Order_Date rows between unbounded preceding and unbounded following)),'2012-12-31 00:00:00.000') time_elapsed_from_last_order
from time_gaps
)
select *,avg(time_elapsed_from_last_order) over() avg_allcust_time_elapsed_last_order,
case when avg_time_gaps is not null and avg_time_gaps > time_elapsed_from_last_order then 'regular' 
	 when avg_time_gaps is null and time_elapsed_from_last_order <= avg(time_elapsed_from_last_order) over () then 'wait' 
	 else 'churn'
	 end as decision
from t5
------------------------------------------------------------PART 3----------------------------------------------------------------------------------------
----Find the number of customers retained_month_wise
create  function number_of_customers_retained_month_wise(@m int , @y int) 
returns int 
as

begin 
		return	(select count(distinct Cust_ID) 
				from time_gaps
				where year(Order_Date) = @y and month(Order_Date) = @m and time_gap = 1)
	
end

---example
select [dbo].[number_of_customers_retained_month_wise](2,2012) number_of_customers_retained_on_third_month


----Calculate the mont wise retention Rate
------- MONT WISE RETENTION RATE calculator (Returns the wise retetion of next month)
create function Month_Wise_Retention_Rate(@m int , @y int) 
returns decimal(10,2)
as

begin
	
	return  
				100*(select
					(select count(distinct Cust_ID) 
					from time_gaps
					where year(Order_Date) = @y and month(Order_Date) = @m and time_gap = 1)*1.00
				     /
					(select count(distinct Cust_ID)
					from time_gaps
					where year(Order_Date) = @y and month(Order_Date) = @m)*1.00)
end

----example
select [dbo].[Month_Wise_Retention_Rate](9,2012) tenth_month_retention_rate





