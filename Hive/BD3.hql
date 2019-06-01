DROP TABLE historical_stock_prices;
CREATE TABLE IF NOT EXISTS historical_stock_prices(ticker STRING,open DOUBLE,close DOUBLE,adj_close DOUBLE,lowThe DOUBLE, highThe DOUBLE,volume BIGINT,data DATE)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");
;

LOAD DATA LOCAL INPATH '/home/ene/Scrivania/Dati-primo-progetto/HSP0.csv' INTO TABLE historical_stock_prices;

drop table if exists historical_stocks;
CREATE TABLE IF NOT EXISTS historical_stocks(ticker STRING,exchangen STRING,name STRING,sector STRING,industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
tblproperties("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '/home/ene/Scrivania/Dati-primo-progetto/HS1.csv' INTO TABLE historical_stocks;

DROP TABLE time;
CREATE TABLE IF NOT EXISTS time AS (SELECT current_timestamp as inizio);

DROP TABLE TickerYearFirstcloseDateLastcloseDate;
CREATE TABLE IF NOT EXISTS TickerYearFirstcloseDateLastcloseDate AS (
SELECT          ticker , YEAR(data) AS year,
                MIN(data) AS FirstcloseDate,
                MAX(data) AS LastcloseDate
FROM historical_stock_prices
WHERE YEAR(data) >= 2016 AND YEAR(data) <= 2018
GROUP BY ticker , YEAR(data)
);

DROP TABLE TickerYearFirstcloseLastclose;
CREATE TABLE IF NOT EXISTS TickerYearFirstcloseLastclose AS (
SELECT          ty.ticker ,ty.year ,MAX(ty.FirstcloseDate),MAX(ty.LastcloseDate),
                SUM(CASE WHEN hsp.data = ty.FirstcloseDate THEN hsp.close ELSE 0 END) AS Firstclose,SUM(CASE WHEN hsp.data = ty.LastcloseDate THEN hsp.close ELSE 0 END) AS Lastclose

FROM historical_stock_prices hsp , TickerYearFirstcloseDateLastcloseDate ty
WHERE ty.ticker = hsp.ticker 
GROUP BY ty.ticker,ty.year
);

DROP TABLE TickerALLYearFirstcloseLastclose;
CREATE TABLE IF NOT EXISTS TickerALLYearFirstcloseLastclose AS (
SELECT          ticker ,
SUM(CASE WHEN year = "2016" THEN firstClose ELSE 0 END) AS firstClose2016,
SUM(CASE WHEN year = "2016" THEN lastClose ELSE 0 END)  AS lastClose2016,
SUM(CASE WHEN year = "2017" THEN firstClose ELSE 0 END) AS firstClose2017,
SUM(CASE WHEN year = "2017" THEN lastClose ELSE 0 END) AS lastClose2017,
SUM(CASE WHEN year = "2018" THEN firstClose ELSE 0 END) AS firstClose2018,
SUM(CASE WHEN year = "2018" THEN lastClose ELSE 0 END) AS lastClose2018

FROM TickerYearFirstcloseLastclose
GROUP BY ticker
);

DROP TABLE FiltroZeriTickerALLYearFirstcloseLastclose;
CREATE TABLE IF NOT EXISTS FiltroZeriTickerALLYearFirstcloseLastclose AS (
SELECT *  FROM TickerALLYearFirstcloseLastclose 
WHERE firstClose2016<>0 OR firstClose2017<>0 OR firstClose2018<>0);

DROP TABLE HSPjoinHS;
CREATE TABLE IF NOT EXISTS HSPjoinHS AS (
SELECT name,firstClose2016,lastClose2016,
firstClose2017,lastClose2017,firstClose2018,lastClose2018,sector
  
FROM FiltroZeriTickerALLYearFirstcloseLastclose hsp , historical_stocks hs 
WHERE hsp.ticker = hs.ticker AND hs.sector<>"N/A");

DROP TABLE groupBYname;
CREATE TABLE IF NOT EXISTS groupBYname AS (
SELECT name,
       SUM(firstClose2016) AS firstClose2016,
       SUM(lastClose2016)  AS lastClose2016,
       SUM(firstClose2017) AS firstClose2017,
       SUM(lastClose2017)  AS lastClose2017,
       SUM(firstClose2018)AS firstClose2018,
       SUM(lastClose2018) AS lastClose2018,
       MAX(sector) AS sector       

FROM HSPjoinHS 
GROUP BY name);

DROP TABLE nameAumentoIncrementaleForAllThreeYears;
CREATE TABLE IF NOT EXISTS nameAumentoIncrementaleForAllThreeYears AS (
SELECT name ,
CAST((((lastClose2016-firstClose2016)/firstClose2016)*100)AS INT) AS aumentoIncrementale2016,
CAST((((lastClose2017-firstClose2017)/firstClose2017)*100)AS INT) AS aumentoIncrementale2017,
CAST((((lastClose2018-firstClose2018)/firstClose2018)*100)AS INT) AS aumentoIncrementale2018,
sector

FROM groupBYname 
);


DROP TABLE results;
CREATE TABLE IF NOT EXISTS results AS (
SELECT a1.aumentoIncrementale2016,a1.aumentoIncrementale2017,
       a1.aumentoIncrementale2018,a1.name AS name1,a2.name AS name2,a1.sector as s1 , a2.sector as s2
FROM 
nameAumentoIncrementaleForAllThreeYears a1,
nameAumentoIncrementaleForAllThreeYears a2
WHERE a1.aumentoIncrementale2016 = a2.aumentoIncrementale2016 AND
      a1.aumentoIncrementale2017 = a2.aumentoIncrementale2017 AND
      a1.aumentoIncrementale2018 = a2.aumentoIncrementale2018 AND
      a1.sector<>a2.sector
);

select * from results;
select current_timestamp - t.inizio from time t;
