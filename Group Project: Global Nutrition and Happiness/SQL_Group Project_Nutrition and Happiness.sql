#----------------------------------------
# BC2402 Group Project
# Done by: Emma
# Qns: 10, 14, 18, 22
#----------------------------------------


# Q10
select item, calories, totalfat, cholestrol, sodium,
	case
		when totalfat > 30 or sodium > 1000 or cholestrol > 30 then '⚠ High Risk'
        else '✓ Moderate'
	end as health_flag
from mcdonaldata
order by health_flag;


# Q 14
select item, type, calories, 
	cast(avg(calories) over (partition by type) as dec(10,0)) as avg_type_calories, 
    cast(calories-avg(calories) over (partition by type) as dec(10,0)) as delta_from_avg
from starbucks;


# Q 18

with nutrient_var as(
	select daily_intake.entity as country,
    stddev(Daily_calorie_animal_protein + Daily_calorie_vegetal_protein + 
			Daily_calorie_fat + Daily_calorie_carbohydrates) as total_calorie_stddev
	from daily_intake
    group by daily_intake.entity
)
select nutrient_var.country, 
		round(nutrient_var.total_calorie_stddev, 2) as `Nutrition Variation`,
		happiness.Happiness_Score, 
        case
			when nutrient_var.total_calorie_stddev < 100 then 'Low Variation'
            when nutrient_var.total_calorie_stddev < 200 then 'Medium Variation'
            else 'High Variation'
		end as `Variation Category`,
        case
			when happiness.Happiness_Score >= 7.0 then 'High Score'
            when happiness.Happiness_Score >= 5.0 then 'Medium Score'
            else 'Low Score'
		end as `Score Category`
from nutrient_var, happiness
where nutrient_var.country = happiness.country
order by nutrient_var.total_calorie_stddev asc;
    
/* SHIQIAN ANSWER */
-- Step 1: Calculate variation (standard deviation) of nutrient intake over time for each country
create temporary table nutrient_variation as
SELECT 
	Entity AS country,
	-- Calculate standard deviation for each nutrient across years
	STDDEV(CAST(Daily_calorie_animal_protein AS DECIMAL(10,4))) AS animal_protein_variation,
	STDDEV(CAST(Daily_calorie_vegetal_protein AS DECIMAL(10,4))) AS vegetal_protein_variation,
	STDDEV(CAST(Daily_calorie_fat AS DECIMAL(10,4))) AS fat_variation,
	STDDEV(CAST(Daily_calorie_carbohydrates AS DECIMAL(10,4))) AS carb_variation
FROM bc2402_gp.daily_intake
GROUP BY Entity;

-- Step 2: Calculate overall average variation across all nutrients
create temporary table overall_variation AS
SELECT 
	country,
	animal_protein_variation,
	vegetal_protein_variation,
	fat_variation,
	carb_variation,
	-- Average variation across all nutrients
	(animal_protein_variation + vegetal_protein_variation + 
	 fat_variation + carb_variation) / 4 AS avg_nutrient_variation
FROM nutrient_variation;

-- Step 3: Get happiness score for each country
create temporary table happiness_data AS
SELECT 
	Country,
	CAST(Happiness_Score AS DECIMAL(10,6)) AS happiness_score
FROM happiness;

-- Step 4: Join and analyze the relationship
create table nutrient_happiness as
SELECT 
    ov.country,
    ROUND(ov.avg_nutrient_variation, 2) AS avg_nutrient_variation,
    ROUND(ov.animal_protein_variation, 2) AS animal_protein_variation,
    ROUND(ov.vegetal_protein_variation, 2) AS vegetal_protein_variation,
    ROUND(ov.fat_variation, 2) AS fat_variation,
    ROUND(ov.carb_variation, 2) AS carb_variation,
    ROUND(h.happiness_score, 3) AS happiness_score   
FROM overall_variation ov
INNER JOIN happiness_data h 
    ON ov.country = h.Country
ORDER BY ov.avg_nutrient_variation ASC;


# Q22

CREATE TEMPORARY TABLE monthly_data AS
SELECT 
	cast(Year as decimal(4,0)) as YearNum,
	cast(Month as decimal(2,0)) as MonthNum,
	AVG(cast(Daily_calorie_fat as decimal(30,20))) as avg_fat,
	AVG(cast(Daily_calorie_carbohydrates as decimal(30,20))) as avg_carb,
	AVG(cast(Daily_calorie_animal_protein as decimal(30,20)) + cast(Daily_calorie_vegetal_protein as decimal(30,20))) as avg_protein
FROM simulated_food_intake_2015_2020
GROUP BY YearNum, MonthNum;


CREATE TEMPORARY TABLE changes AS
SELECT 
	YearNum, MonthNum, avg_fat, avg_carb, avg_protein,
	-- Get previous month's value
	LAG(avg_fat) OVER (ORDER BY YearNum, MonthNum) as prev_fat,
	LAG(avg_carb) OVER (ORDER BY YearNum, MonthNum) as prev_carb,
	LAG(avg_protein) OVER (ORDER BY YearNum, MonthNum) as prev_protein
FROM monthly_data;

-- calculate changes per month
SELECT 
    YearNum as Year, concat(MonthNum - 1, " --> ", MonthNum) as Month,
    ROUND(avg_fat - prev_fat, 2) as fat_change,
    ROUND(avg_carb - prev_carb, 2) as carb_change,
    ROUND(avg_protein - prev_protein, 2) as protein_change
FROM changes
ORDER BY YearNum, MonthNum;


-- Assign seasonal/holiday pattern
CREATE TEMPORARY TABLE seasonal_patterns AS
SELECT 
    concat(MonthNum - 1, " --> ", MonthNum) as Month,
    CASE 
        WHEN MonthNum IN (11, 12) THEN 'Holiday Season'
        WHEN MonthNum IN (1, 2) THEN 'Post-Holiday'
        WHEN MonthNum IN (6, 7, 8) THEN 'Summer'
        WHEN MonthNum = 9 THEN 'Back-to-School'
		WHEN MonthNum = 10 THEN 'Fall'
        WHEN MonthNum IN (3, 4, 5) THEN 'Spring'
        ELSE 'NIL'
    END as season_holiday,
    ROUND(avg(avg_fat - prev_fat), 2) as avg_fat_change,
    ROUND(avg(avg_carb - prev_carb), 2) as avg_carb_change,
    ROUND(avg(avg_protein - prev_protein), 2) as avg_protein_change
FROM changes
GROUP BY MonthNum, season_holiday;

select * from seasonal_patterns;

-- ONLY IN CHANGES > 0, 
-- Identify What Drives Each Pattern: Is it fat? Carbs? Protein?
SELECT 
    Month, season_holiday, avg_fat_change, avg_carb_change, avg_protein_change,
    -- Identify the dominant driver
    CASE 
        WHEN avg_fat_change > avg_carb_change AND avg_fat_change > avg_carb_change 
            THEN 'FAT'
        WHEN avg_carb_change > avg_fat_change AND avg_carb_change > avg_protein_change
            THEN 'CARB'
        ELSE 'PROTEIN'
    END as problem_nutrient
FROM seasonal_patterns
WHERE season_holiday IN ('Post-Holiday', 'Holiday Season')
ORDER BY avg_fat_change + avg_carb_change + avg_protein_change DESC;



/* SECTION 2: Identifying Suitable Food Options from Fast Food Chains */

create temporary table food_category as
SELECT restaurant, item,
    total_fat, total_carb,
    -- Fat category (based on standards: High > 17.5g ; Low <= 3g of fat or less per 100g)
    -- Assuming average serving is ~250g, High > ~43.75g ; Low <= ~7.5g of total fat
    CASE 
        WHEN total_fat > 43.75 THEN 'HIGH-FAT'
        WHEN total_fat <= 7.5 THEN 'LOW-FAT'
        ELSE 'MODERATE-FAT'
    END AS fat_category,
    -- Carb category (based on standards: High > 22.5g ; Low <= 5g of total sugars per 100g)
	-- Assuming average serving is ~250g, High > ~56.25g ; Low <= 12.5g of total sugars
    CASE 
        WHEN total_carb > 56.25 THEN 'HIGH-CARB'
        WHEN total_carb <= 12.5 THEN 'LOW-CARB'
        ELSE 'MODERATE-CARB'
    END AS carb_category
    -- Overall health rating
FROM fastfood
ORDER BY total_fat, total_carb;

select * from food_category;

SELECT 
    restaurant,
    COUNT(*) AS total_items,
    ROUND(AVG(total_fat), 2) AS avg_fat,
    ROUND(AVG(total_carb), 2) AS avg_carb,
    -- Combined health score (lower is better)
    ROUND(AVG(total_fat) + AVG(total_carb), 2) AS combined_score,
    -- Count healthy options
    SUM(CASE WHEN total_fat <= 7.5 AND total_carb <= 12.5 THEN 1 ELSE 0 END) AS healthy_items_count
FROM food_category
WHERE fat_category = 'LOW-FAT' and carb_category = 'LOW-CARB'
GROUP BY restaurant
ORDER BY combined_score ASC;


CREATE TEMPORARY TABLE top3_low_carb_restaurants AS
SELECT 
    restaurant,
    ROUND(AVG(total_carb), 2) AS avg_carb,
    COUNT(*) AS total_items
FROM food_category
WHERE carb_category = 'LOW-CARB'
GROUP BY restaurant
ORDER BY avg_carb ASC
LIMIT 3;


CREATE TEMPORARY TABLE top3_low_fat_restaurants AS
SELECT 
    restaurant,
    ROUND(AVG(total_fat), 2) AS avg_fat,
    COUNT(*) AS total_items
FROM food_category
WHERE fat_category = 'LOW-FAT'
GROUP BY restaurant
ORDER BY avg_fat ASC
LIMIT 3;


SELECT 'LOW-CARB Partners (Oct-Jan Campaigns)' AS campaign_type, restaurant, avg_carb AS avg_nutrient
FROM top3_low_carb_restaurants
UNION ALL
SELECT 'LOW-FAT Partners (Jan-Feb Campaign)' AS campaign_type, restaurant, avg_fat AS avg_nutrient
FROM top3_low_fat_restaurants;


-- Top 5 lowest-carb items from each of the top 3 low-carb restaurants
WITH ranked_items AS (
    SELECT 
        f.restaurant, f.item, f.total_fat, f.total_carb,
		ROW_NUMBER() OVER (PARTITION BY f.restaurant ORDER BY f.total_carb ASC) AS item_rank
    FROM food_category f
    INNER JOIN top3_low_carb_restaurants t ON f.restaurant = t.restaurant
)
SELECT 
    'CARB-DRIVEN Campaigns (Oct-Jan)' AS campaign_period,
    restaurant, item, total_fat, total_carb, item_rank
FROM ranked_items
WHERE item_rank <= 5
ORDER BY restaurant, item_rank;

-- Top 5 lowest-fat items from each of the top 3 low-fat restaurants
WITH ranked_items AS (
    SELECT 
        f.restaurant, f.item, f.total_fat, f.total_carb,
		ROW_NUMBER() OVER (PARTITION BY f.restaurant ORDER BY f.total_fat ASC) AS item_rank
    FROM food_category f
    INNER JOIN top3_low_fat_restaurants t ON f.restaurant = t.restaurant
)
SELECT 
    'FAT-DRIVEN Campaign (Jan-Feb)' AS campaign_period,
    restaurant, item, total_fat, total_carb, item_rank
FROM ranked_items
WHERE item_rank <= 5
ORDER BY restaurant, item_rank;




