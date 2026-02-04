/*
BC2402 Designing and Developing Databases
Individual Assignment

Name: Quek Minn, Emma
Student ID: U2410898A
*/

-- -----------------------------------------------------------------------------------
/* 1 */
-- Q: How many product categories are there?
select count(distinct product_category)
from baristacoffeesalestbl;
/* There are 7 categories. */

-- Q: For each product category, show the number of records.
select product_category, count(*) as no_of_records
from baristacoffeesalestbl
group by product_category;

-- -----------------------------------------------------------------------------------
/* 2 */
select customer_gender, loyalty_member, 
	sum(count(*)) over (partition by customer_gender, loyalty_member) as records,
	is_repeat_customer, count(*) as records
from baristacoffeesalestbl
group by customer_gender, loyalty_member, is_repeat_customer
order by customer_gender, is_repeat_customer desc;

/*
Simply putting count(*) will result in considering is_repeat_customer as a group as well.
Hence to get the records only considering customer_gender and loyalty_member, I used a 
window function sum(count(*)) over (partition by ...).
-> sum(count(*)) counts the total records within each subgroup stated afterwards.
-> my subgroup, indicated using partition by includes only customer_gender and loyalty_member.
*/

-- -----------------------------------------------------------------------------------
/* 3 */
-- Version A
select product_category, customer_discovery_source, 
	sum(cast(total_amount as decimal(5,0))) as total_sales
from baristacoffeesalestbl
group by product_category, customer_discovery_source
order by product_category, customer_discovery_source;

-- Version B
select product_category, customer_discovery_source, 
	sum(total_amount) as total_sales
from baristacoffeesalestbl
group by product_category, customer_discovery_source
order by product_category, customer_discovery_source;

/* Q: discuss the reasons for the differences and indicate which version is correct.
Version A rounds each single value first, by converting text into a number 
(decimal 0 in this case).
Version B takes the values in text form, including decimals and does not round them off.

Version B is more accurate, as seen from the results of rounding that the value increases
by a significant amount due to the accumulation of each rounded value.
*/

-- -------------------------------------------------------------------------------------
/* 4 */
create or replace view coffee_normalised as                              -- A
    select 
        case
            when (time_of_day_morning = 'True') then 'morning'
            when (time_of_day_afternoon = 'True') then 'afternoon'
            when (time_of_day_evening = 'True') then 'evening'
        end as time_of_day,
        case
            when (gender_female = 'True') then 'female'
            when (gender_male = 'True') then 'male'
        end as gender,
        case
            when (beverage_coffee = 'True') then 'coffee'
            when (beverage_energy_drink = 'True') then 'energy drink'
            when (beverage_tea = 'True') then 'tea'
		end as beverage,
		focus_level,
		sleep_quality
    from caffeine_intake_tracker;
        
select time_of_day, gender,                                             -- B
	cast(avg(focus_level) as decimal(5,4)) as avg_focus_level,
    cast(avg(sleep_quality) as decimal(5,4)) as avg_sleep_quality      
from coffee_normalised
where beverage = 'coffee'                                               -- C
group by time_of_day, gender
order by field(time_of_day,'morning','afternoon','evening'), gender;    -- D

/*
A) Looking at the original caffeine_intake_tracker table, values from time_of_day, gender, 
and beverage are stored as booleans, that is, for each category there is a separate column,
which indicates True or False according to whether the record fits under such category.
Assuming that within each of these categories the booleans are mutally exclusive, I decided
to create a view to merge categories into a single column for grouping and naming purposes.
I also included the 2 targeted numerical columns focus_level and sleep_quality in the view.

B) Then, from the normalised view, used a select clause to extract time_of_day and gender,
and converting average of focus_level and sleep_quality to decimal data type with 5 d.p..

C) I filtered and selected only records that are beverage type coffee.

D) Finally, I used field() to re-order time_of_day to the order according to the 
sequential time of day. Without this re-ordering, the automatic ordering system would be 
according to alphabetical order, which is the incorrect ordering convention.
*/

-- -------------------------------------------------------------------------------------
/* 5 */
select 
	case
		when (hour(str_to_date(datetime, '%H:%i.%s')) % 24) < 12 
         then 'Before 12' 
		else 'After 12' 
	end as period,
  cast(sum(money) as decimal(10,2)) as amt
from coffeesales
group by period;

/*
There are 2 issues:
1) format of datetime column is text and structured unconventionally as HH:MM:S.
	This question only asks for before or after 12, hence we are interested in HH only.

	I solved it by using str_to_date() to convert text into time, then used hour() to extract HH.

2) there are some values in datetime that are > 24, which are more than the hours of the day

	I solved it by assuming those are overrun values that spill into the next day 
	i.e. 24.00.1 on 1/3/2024 is 1 second spilled to 2/3/2024 -> it is considered to be 'Before 12'
	I hence used modulus division to divide all values by 24, ending up with the "leftover" hours spilled
*/

/* 6 */
with recursive bins(i) as (                                               -- A
	select 0 union all
    select i+1 from bins where i<6
)
select
	concat(i,' to ',i+1) as Ph,                                           -- B
	cast(avg(Liking) as decimal(3,2)) as avgLiking,
	cast(avg(FlavorIntensity) as decimal(3,2)) as avgFlavorIntensity,
	cast(avg(Acidity) as decimal(3,2)) as avgAcidity,
	cast(avg(Mouthfeel) as decimal(3,2)) as avgMouthfeel
from bins
left join consumerpreference                                              -- C
	on pH >= i and pH < i+1
group by i
order by i;

/*
A) I used "with recursive...as" to create the sequence of pH bins.
How it works is similar to with...as, letting user define a temporary "virtual table" to
reference in the same query, except it allows the expression to reference itself as well.
This allows the user to generate sequences/ hierachies.
Breaking it down, 
-> "select 0" is the anchor query, that is it produces the first row of the sequence: i=0
-> "union all select i+1 from bins" is the recursive query, it takes the latest value of i
and adds 1 to it, then emits the next row.
-> "where i<6" is the limiting value which ends the recursion at 6.

B) This serves purely naming purposes. Concat() is a string function that joins strings into
one. In this case, it takes the starting number of each bin, joins 'to', along with the 
number after that.

C) The main query works by pulling matching rows from consumerpreference table where pH
falls betweens that interval, that is equal or above the stated bin or lower than one 
integer value above that. Left join is used to ensure bins with no matching rows, hence
showing NULL, still shows up.
*/ 

-- -------------------------------------------------------------------------------------
/* 7 */
create or replace view ranked_sales as
select 
	upper(date_format(str_to_date(date, '%d/%m/%Y'), '%b')) as trans_month,     -- B  
	store_id,
	store_location,
	location_name, 
	cast(                                                                       -- C
		avg((cast(substring_index(agtron, '/', 1) as decimal(20,10)) +
			 cast(substring_index(agtron, '/', -1) as decimal(20,10))) /2) 
	as decimal(15,6)) as avg_agtron, 
	count(*) as trans_amt,
    cast(sum(money) as decimal(10,2)) as total_money,                           -- D
	row_number() over(                                                          -- E
		partition by upper(date_format(str_to_date(date, '%d/%m/%Y'), '%b'))    
		order by sum(money) desc
	) as ranked
from coffeesales, list_coffee_shops_in_kota_bogor, `top-rated-coffee`, baristacoffeesalestbl
where coffeesales.coffeeID = `top-rated-coffee`.ID and
	coffeesales.shopID = list_coffee_shops_in_kota_bogor.no and
	coffeesales.customer_id = substring_index(baristacoffeesalestbl.customer_id, '_', -1)    -- A
group by trans_month, store_id, store_location, location_name;

select trans_month, store_id, store_location, location_name,
	avg_agtron, trans_amt, total_money
from ranked_sales
where ranked <= 3                                                      -- F
order by field(trans_month, 'MAR', 'APR', 'MAY', 'JUN', 'JUL');        -- G

/*
I decided to create a view that groups according to month and orders by ranking of highest 
sum of money to lowest, before extracting the top 3 in the main query.

A) To join the tables coffeesales and baristacoffeesalestbl whose foreign keys are not 
matching, I assumed the number that followed "CUST_" in baristacoffeesalestbl corresponded
to the numerical customer_id in coffeesales.
Hence, I used substring_index(), which performs string splitting, allowing you to split
a string by a delimeter then return part of it.
The general form is as such: substring_index(str, delim, count)
In this case, the string is the customer_id col, the delimeter to split by is '_', and
the count takes in either 1 or -1. 1 = returns everything to the left; -1 = returns
everything to the right. Hence in this case I used -1 to return the numerical value
situated on the right of the '_'.

B) To handle the date column, there are various steps:
-> First, I used str_to_date() to convert the string into proper SQL date format, '%d/%m/%Y'.
-> date_format(..., format) takes the date and formats it into a string, according to
the format specified, you could return abbreviated or full weekday name, month name etc.
In this case, '%b' returns abbreviated month name.
-> Finally, upper() converts a string to all uppercase, in this case the abbreviated month
name.

C) The agtron value is a numeric scale to measure the roast level of coffee beans. It 
often measures 2 values: the first being the outside of the roasted beans and the second
the roasted grounds. If indicated as one value, the 2 are often averaged.
[source: https://huladaddy.com/blogs/blog/whats-your-agtron-number]
Hence, I assumed the 2 agtron values needed to be averaged i.e. xx/yy ==> (xx + yy) /2.
I used substring_index() (using same theory as point A) to extract the 2 values to the 
right and left of the delimeter, "/" in this case.
I also noted to convert the data type from text to decimal when performing calculations
using cast( ... as decimal() )

D) For calculate total money, I used sum().
Also making sure to convert the data type to decimal using cast( ... as decimal() ).

E) To order and group the values at the same time, I decided to use row_number() over().
Firstly, I used partition by() to group the values according to month by copying the
previous formula I used to extract month.
Then, I used order by() to rank the groups by the sum of total money.
I named this as rank.

F) In the main query, I extracted the columns I wanted from the view.
Then, I filtered only the first 3 'ranked' values, hence extracting the top 3 per month.

G) I used field() to order the months by sequential order since the default ordering
convention is by alphabetical order, which is not the right sequence.
*/
