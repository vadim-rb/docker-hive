CREATE TABLE gtd_table (
  eventid BIGINT,
  iyear INT,
  imonth INT,
  iday INT,
  approxdate STRING,
  extended_ INT,
  resolution STRING,
  country INT,
  country_txt STRING,
  region INT,
  region_txt STRING,
  provstate STRING,
  city STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  specificity INT,
  vicinity INT,
  location STRING,
  summary STRING,
  crit1 STRING,
  crit2 STRING,
  crit3 STRING,
  doubtterr STRING,
  alternative STRING,
  alternative_txt STRING,
  multiple STRING,
  success STRING,
  suicide STRING,
  attacktype1 STRING,
  attacktype1_txt STRING,
  attacktype2 STRING,
  attacktype2_txt STRING,
  attacktype3 STRING,
  attacktype3_txt STRING,
  targtype1 STRING,
  targtype1_txt STRING,
  targsubtype1 STRING,
  targsubtype1_txt STRING,
  corp1 STRING,
  target1 STRING,
  natlty1 STRING,
  natlty1_txt STRING,
  targtype2 STRING,
  targtype2_txt STRING,
  targsubtype2 STRING,
  targsubtype2_txt STRING,
  corp2 STRING,
  target2 STRING,
  natlty2 STRING,
  natlty2_txt STRING,
  targtype3 STRING,
  targtype3_txt STRING,
  targsubtype3 STRING,
  targsubtype3_txt STRING,
  corp3 STRING,
  target3 STRING,
  natlty3 STRING,
  natlty3_txt STRING,
  gname STRING,
  gsubname STRING,
  gname2 STRING,
  gsubname2 STRING,
  gname3 STRING,
  gsubname3 STRING,
  motive STRING,
  guncertain1 STRING,
  guncertain2 STRING,
  guncertain3 STRING,
  individual STRING,
  nperps STRING,
  nperpcap STRING,
  claimed STRING,
  claimmode STRING,
  claimmode_txt STRING,
  claim2 STRING,
  claimmode2 STRING,
  claimmode2_txt STRING,
  claim3 STRING,
  claimmode3 STRING,
  claimmode3_txt STRING,
  compclaim STRING,
  weaptype1 STRING,
  weaptype1_txt STRING,
  weapsubtype1 STRING,
  weapsubtype1_txt STRING,
  weaptype2 STRING,
  weaptype2_txt STRING,
  weapsubtype2 STRING,
  weapsubtype2_txt STRING,
  weaptype3 STRING,
  weaptype3_txt STRING,
  weapsubtype3 STRING,
  weapsubtype3_txt STRING,
  weaptype4 STRING,
  weaptype4_txt STRING,
  weapsubtype4 STRING,
  weapsubtype4_txt STRING,
  weapdetail STRING,
  nkill STRING,
  nkillus STRING,
  nkillter STRING,
  nwound STRING,
  nwoundus STRING,
  nwoundte STRING,
  property STRING,
  propextent STRING,
  propextent_txt STRING,
  propvalue STRING,
  propcomment STRING,
  ishostkid STRING,
  nhostkid STRING,
  nhostkidus STRING,
  nhours STRING,
  ndays STRING,
  divert STRING,
  kidhijcountry STRING,
  ransom STRING,
  ransomamt STRING,
  ransomamtus STRING,
  ransompaid STRING,
  ransompaidus STRING,
  ransomnote STRING,
  hostkidoutcome STRING,
  hostkidoutcome_txt STRING,
  nreleased STRING,
  addnotes STRING,
  scite1 STRING,
  scite2 STRING,
  scite3 STRING,
  dbsource STRING,
  INT_LOG STRING,
  INT_IDEO STRING,
  INT_MISC STRING,
  INT_ANY STRING,
  related STRING
) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/opt/hive/examples/files/gtd.csv' OVERWRITE INTO TABLE gtd_table;

ALTER TABLE gtd_table SET TBLPROPERTIES ("skip.header.line.count"="1");

--drop table gtd_table


--На этих данных построить витрины (5-6) с использованием конструкций: where, count, group by, having, order by, join, union, window.

--1. Распределение тер.атак по странам по годам

drop table attack_by_country_year

CREATE TABLE attack_by_country_year AS 
SELECT 
	country_txt,iyear,count(*) as attack_counts
FROM
	gtd_table
GROUP BY
	country_txt,iyear
HAVING  
	country_txt rlike '[^0-9]'

SELECT * FROM attack_by_country_year LIMIT 10

drop table killed_by_year

--2. Кол-во убийств по годам
CREATE TABLE killed_by_year AS 
SELECT
	iyear,sum(nkill) as kills
FROM
	gtd_table
GROUP BY
	iyear
ORDER BY 
	iyear
	
SELECT * FROM killed_by_year LIMIT 10

drop table killed_by_country

--3. Кол-во убийств по странам по годам
CREATE TABLE killed_by_country AS 
SELECT
	country_txt,iyear,sum(nkill) as kills
FROM
	gtd_table
GROUP BY
	country_txt,iyear
HAVING  
	country_txt rlike '[^0-9]'

SELECT * FROM killed_by_country LIMIT 10	
	
--4. Процентное соотношение убийств в стране относительно кол-ву убийств в мире
CREATE TABLE procent_killed_by_country AS 
SELECT
	ace.country_txt,
	ace.iyear,
	ace.attack_counts,
	ky.kills as world_sum_kills,
	kc.kills as kill_in_country,
	round((kc.kills/ky.kills)*100,1) as percent_of_kills
FROM 
	attack_by_country_year ace
	JOIN killed_by_year ky ON ky.iyear=ace.iyear
	JOIN killed_by_country kc ON kc.country_txt = ace.country_txt and kc.iyear=ace.iyear
	
--5. Кол-во убийств по странам по годам c итоговой записью
CREATE TABLE killed_by_country_with_result_row AS 
SELECT country_txt,iyear,kills FROM killed_by_country 
UNION ALL
SELECT "ALL",max(iyear),sum(kills) FROM killed_by_country 

drop table first_attack_country
--6. Найти первый год, когда произошла тер. атака в стране
CREATE TABLE first_attack_country AS 
with cte as (
SELECT 
	country_txt,
	iyear,
	row_number() OVER (PARTITION BY country_txt ORDER BY iyear) as rn
FROM 
	gtd_table 
WHERE 
	country_txt rlike '[^0-9]'
)
select 
	country_txt,
	iyear
from 
	cte 
where 
	rn=1

SELECT * from first_attack_country
	




