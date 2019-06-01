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
                MAX(data) AS LastcloseDate,
                SUM(volume) AS sumVolume,
                SUM(close) AS sumClose,
                COUNT(1) AS conta

FROM historical_stock_prices
WHERE YEAR(data) >= 2004 AND YEAR(data) <= 2018
GROUP BY ticker , YEAR(data)
);

DROP TABLE TickerYearFirstcloseLastclose;
CREATE TABLE IF NOT EXISTS TickerYearFirstcloseLastclose AS (
SELECT          ty.ticker ,ty.year ,MAX(ty.FirstcloseDate),MAX(ty.LastcloseDate),
                SUM(CASE WHEN hsp.data = ty.FirstcloseDate THEN hsp.close ELSE 0 END) AS Firstclose,
                SUM(CASE WHEN hsp.data = ty.LastcloseDate THEN hsp.close ELSE 0 END) AS Lastclose,
                SUM(CASE WHEN hsp.data = ty.FirstcloseDate THEN ty.sumVolume ELSE 0 END) AS sumVolume,
                SUM(CASE WHEN hsp.data = ty.LastcloseDate THEN ty.sumClose ELSE 0 END) AS sumClose,
                SUM(CASE WHEN hsp.data = ty.FirstcloseDate THEN ty.conta ELSE 0 END) AS conta

               
FROM historical_stock_prices hsp , TickerYearFirstcloseDateLastcloseDate ty
WHERE ty.ticker = hsp.ticker AND YEAR(data) >= 2004 AND YEAR(data) <= 2018
GROUP BY ty.ticker,ty.year
);


DROP TABLE CLOSEVOLUMEjoinHS;
CREATE TABLE IF NOT EXISTS CLOSEVOLUMEjoinHS AS (
SELECT          cjv.ticker , year, sector,
                firstClose,lastClose,sumVolume,sumClose,conta
FROM TickerYearFirstcloseLastclose cjv , historical_stocks hs
WHERE hs.ticker = cjv.ticker AND sector<>"N/A"
);


DROP TABLE GroupBySectorYear;
CREATE TABLE IF NOT EXISTS GroupBySectorYear AS (
SELECT          sector,year,
                SUM(firstClose) AS sumFirstClose,
                SUM(lastClose) AS sumLastClose,
                SUM(sumVolume) AS sumVolume,
                SUM(sumClose) AS sumClose,
                SUM(conta) AS sumConta
FROM CLOSEVOLUMEjoinHS
GROUP BY sector,year 
);


DROP TABLE results;
CREATE TABLE IF NOT EXISTS results AS (
SELECT          sector,year,
                CAST((((sumLastClose-sumFirstClose)/sumFirstClose)*100)AS INT),
                sumVolume,sumClose/sumConta
FROM GroupBySectorYear

);

select * from results;

select current_timestamp - t.inizio from time t;
