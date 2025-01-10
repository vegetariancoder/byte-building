-- use database
use database TELCO_PROJECT;


-- use schema
use schema TELCO_STAGING_TABLES;

-- Identify the top 5 city with the highest churn rates by joining _Status_Analysis.csv_ and _Location_Data.csv_. Plot the churn rate by state.

select * from LOCATION;
select * from STATUS_ANALYSIS;

with top5_city_churn as (
select
    CITY,
    round(avg(CHURN_SCORE),2) as avg_churn_score,
    dense_rank() over (order by avg(CHURN_SCORE) desc ) as drnk
from
    LOCATION
inner join
    STATUS_ANALYSIS
on
    LOCATION.CUSTOMER_ID = STATUS_ANALYSIS.CUSTOMER_ID
where
    CHURN_LABEL = 'Yes' and CHURN_CATEGORY != 'Not Applicable'
group by CITY )
select
    CITY,
    avg_churn_score
from
    top5_city_churn
where
    drnk <=5;