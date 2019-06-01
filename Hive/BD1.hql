DROP TABLE historical_stock_prices;
CREATE TABLE IF NOT EXISTS historical_stock_prices(ticker STRING,open DOUBLE,close DOUBLE,adj_close DOUBLE,lowThe DOUBLE, highThe DOUBLE,volume BIGINT,data DATE)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");
;

LOAD DATA LOCAL INPATH '/home/ene/Scrivania/Dati-primo-progetto/HSP0.csv' INTO TABLE historical_stock_prices;

DROP TABLE time;
CREATE TABLE IF NOT EXISTS time AS (SELECT current_timestamp as inizio);


DROP TABLE tickerGroupBy;
CREATE TABLE IF NOT EXISTS tickerGroupBy AS (
SELECT          ticker ,
                MIN(data) AS FirstcloseDate,
                MAX(data) AS LastcloseDate
                
FROM historical_stock_prices
WHERE YEAR(data) >= 1998 AND YEAR(data) <= 2018
GROUP BY ticker
);

DROP TABLE result;
CREATE TABLE IF NOT EXISTS result AS (
SELECT          hsp.ticker ,
                SUM(CASE WHEN data = FirstcloseDate THEN close ELSE 0 END) AS FirstClose,
                SUM(CASE WHEN data = LastcloseDate THEN close ELSE 0 END) AS LastClose,
                MIN(lowThe) AS minPrice,
                MAX(highThe) AS maxPrice,
                AVG(volume) AS avgVolume
                
                
FROM historical_stock_prices hsp , tickerGroupBy tgb
WHERE YEAR(data) >= 1998 AND YEAR(data) <= 2018 AND tgb.ticker = hsp.ticker
GROUP BY hsp.ticker
);

DROP TABLE risultato;
CREATE TABLE IF NOT EXISTS risultato AS (
SELECT          ticker ,
                CAST((((LastClose-FirstClose)/FirstClose)*100)AS INT) AS aumentoPercentuale,
                minPrice,
                maxPrice,
                avgVolume   
                
FROM result
ORDER BY aumentoPercentuale DESC
LIMIT 10
);

select * from tickerGroupBy;
select * from risultato;

select current_timestamp - t.inizio from time t;
