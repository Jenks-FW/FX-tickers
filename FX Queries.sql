SELECT *--DISTINCT(unit_type), [description]
FROM Staging.dbo.indx_index_definition
WHERE ticker = 'fx' --id = 4884

SELECT TOP(100) *
FROM Staging.dbo.indx_granularity_levels


SELECT TOP (1000) *
  FROM Staging.dbo.indx_granularity_item
  WHERE [id] = 319971--granularity_level_id = 58;

SELECT TOP (1000) *
  FROM Factset.ref_v2.iso_currency_map
  WHERE active = 1;

SELECT TOP (1000) *
  FROM Factset.ref_v2.fx_rates_usd
  ORDER BY [date] DESC;

SELECT *
FROM Staging.dbo.indx_index_data
WHERE index_id = 4884
ORDER BY data_timestamp DESC

-- For Data Eng  
/*INSERT INTO [Staging].[dbo].[indx_index_definition] 
VALUES ('Global Currency Exchange Rates', 'FX', 'D', 'RATIO', 'Currency Exchange Rates', 0, 1, 4, 0, 1, 0, 'Index', 'FactSet', NULL, NULL, NULL, 'Ratio')							
					
INSERT INTO Staging.dbo.indx_granularity_levels([id], [granularity_name], [description], [code])
VALUES(58, 'Currency', 'National Currencies', 'CURRENCY');*/
  
  /* EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  EXAMPLE  
  CREATE TABLE #TEST(granularity_level_id int, granularity1 VARCHAR(255), granularity2 VARCHAR(255), [Description] Varchar(255), ShapeFile Varchar(255))
  INSERT INTO #TEST(granularity_level_id, granularity1, granularity2, [Description], ShapeFile)
  VALUES(51, 'EUR', NULL, 'Euro', NULL),
  (51, 'USD', NULL, 'US DOllar', NULL)

  SELECT * FROM #TEST
 
  
  DROP TABLE IF EXISTS #TEST

  SELECT 58 AS granularity_level_id, iso_currency AS granularity1, NULL AS granularity2, currency_desc AS [Description], NULL AS Shape_File
  --INTO #TEST
  FROM Factset.ref_v2.iso_currency_map
  WHERE active = 1;


  DELETE FROM #TEST
  WHERE granularity1 IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', '999')
  SELECT * FROM #TEST;
  /*
  'BOV' - Bolivian unit of account adjusted for inflation relative to USD, does not circulate
  'CLF' - Chilean unit of account
  'COU' - Columbian unit of account, does not circulate
  'CUC' - 1 CUC = 1 USD
  'GBX' - GBP = 100 * GBX
  'ILX' - ILS = 100 * ILX
  'UYW' - Uruguay unit of account
  'UYI' - Uruguay unit of account 
  '999' - N/A
  */
 
 SELECT TOP 1000 * FROM Factset.ref_v2.iso_currency_map WHERE currency_desc <> 'N/A' AND active = '1'
 SELECT TOP 1000 * FROM Staging.dbo.indx_granularity_item WHERE granularity_level_id = 58
 SELECT TOP 1000 * FROM Factset.ref_v2.fx_rates_usd 
 WHERE date > '2019-01-01'
 AND iso_currency = 'CAD'


-- Table formatted to match columns of [Staging].[dbo].[indx_index_data_check_results]
SELECT TOP 1000 fx.[date] as data_timestamp, exch_rate_usd as data_value, gi.[id] as granularity_item_id, 4789 as index_id
FROM Factset.ref_v2.iso_currency_map AS cm
	INNER JOIN Staging.dbo.indx_granularity_item AS gi 
		ON cm.currency_desc = gi.[Description] COLLATE SQL_Latin1_General_CP1_CI_AS
	INNER JOIN Factset.ref_v2.fx_rates_usd AS fx
		ON cm.iso_currency = fx.iso_currency
WHERE gi.granularity_level_id = 58
AND cm.iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', '999')
AND cm.active = '1'
ORDER BY granularity_item_id DESC


-- Attempt at creating a temp table that would get inserted into staging
DROP TABLE IF EXISTS #FOREX
CREATE TABLE #FOREX(data_timestamp date, data_value VARCHAR(255), granularity_item_id VARCHAR(255), index_id VARCHAR(255))
INSERT INTO #FOREX(fx.[date], exch_rate_usd, gi.[id], 4789)
FROM Factset.ref_v2.iso_currency_map AS cm
	INNER JOIN Staging.dbo.indx_granularity_item AS gi 
		ON cm.currency_desc = gi.[Description] COLLATE SQL_Latin1_General_CP1_CI_AS
	INNER JOIN Factset.ref_v2.fx_rates_usd AS fx
		ON cm.iso_currency = fx.iso_currency
WHERE gi.granularity_level_id = 58
AND cm.iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', '999')
AND cm.active = '1'
ORDER BY data_timestamp DESC

SELECT COUNT(DISTINCT fx.iso_currency)--, [date], exch_rate_usd, exch_rate_per_usd
FROM Factset.ref_v2.fx_rates_usd AS fx
INNER JOIN Factset.ref_v2.iso_currency_map AS cm
ON cm.iso_currency = fx.iso_currency
WHERE cm.active = 1
AND fx.iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999');

SELECT [id] AS granularity_item_id, granularity1, [Description]
                       FROM Staging.dbo.indx_granularity_item
                       WHERE granularity_level_id = 58



/*
SELECT TOP 1000 *
FROM Staging.dbo.indx_index_data
WHERE [index_id] = 4787
*/


-- Don't use for FOREX but could be useful for other tickers
INSERT INTO Staging.dbo.indx_index_data(data_timestamp, data_value, granularity_item_id, index_id)
SELECT data_timestamp, data_value, granularity_item_id, index_id FROM #TEST
--Delete duplicate data from Staging, keeping the newest create date. 
WITH dedup AS (
    SELECT data_timestamp, data_value, granularity_item_id, index_id, createdate, 
	ROW_NUMBER() OVER (PARTITION BY index_id, granularity_item_id, data_timestamp 
	ORDER BY createdate DESC) as rn
    FROM Staging.dbo.indx_index_data
    WHERE index_id IN (SELECT Index_ID FROM @Index_ID_List)
)
DELETE
FROM dedup
WHERE rn > 1
---------------------------------------


--DROP TABLE IF EXISTS #TmpTable
SELECT TOP (1000) iso_currency, [date], exch_rate_usd, exch_rate_per_usd
--INTO #TmpTable
  FROM Factset.ref_v2.fx_rates_usd
  WHERE exch_rate_usd > 1
  --AND [date] > '2000-01-01'
  AND iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999')
  ORDER BY [date], exch_rate_usd DESC;

SELECT CONCAT(iso_currency, 'USD') as granularity1, [date], exch_rate_usd FROM #TmpTable
SELECT CONCAT('USD', iso_currency) as granularity1, [date], exch_rate_per_usd FROM #TmpTable

--For Data Eng
--INSERT INTO Staging.dbo.indx_granularity_item(id, granularity_level_id, granularity1, granularity2, [Description], ShapeFile)
SELECT 58 AS granularity_level_id, CONCAT(iso_currency, 'USD') AS granularity1, NULL AS granularity2, CONCAT(currency_desc, ' to USD') AS [Description], NULL AS Shape_File
  FROM Factset.ref_v2.iso_currency_map
  WHERE active = 1
  AND iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999');

--INSERT INTO Staging.dbo.indx_granularity_item(id, granularity_level_id, granularity1, granularity2, [Description], ShapeFile)
SELECT 58 AS granularity_level_id, CONCAT('USD', iso_currency) AS granularity1, NULL AS granularity2, CONCAT('USD to ', currency_desc) AS [Description], NULL AS Shape_File
  FROM Factset.ref_v2.iso_currency_map
  WHERE active = 1
  AND iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999');

  DROP TABLE IF EXISTS #TestTable
SELECT TOP (1000) iso_currency, [date], exch_rate_usd, exch_rate_per_usd
INTO #TestTable
  FROM Factset.ref_v2.fx_rates_usd
  WHERE exch_rate_usd > 1
  --AND [date] > '2000-01-01'
  AND iso_currency NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999')
  ORDER BY [date], exch_rate_usd DESC;

SELECT DISTINCT CONCAT(iso_currency, 'USD') as granularity1, [date], exch_rate_usd FROM #TestTable
SELECT CONCAT('USD', iso_currency) as granularity1, [date], exch_rate_per_usd FROM #TestTable


SELECT 4789 as index_id, gi.[id] as granularity_item_id, fx.[date] as data_timestamp, exch_rate_usd as data_value
                          FROM Factset.ref_v2.iso_currency_map AS cm
                          	INNER JOIN Staging.dbo.indx_granularity_item AS gi
                          		ON cm.currency_desc = gi.[Description] COLLATE SQL_Latin1_General_CP1_CI_AS
                          	INNER JOIN Factset.ref_v2.fx_rates_usd AS fx
                          		ON cm.iso_currency = fx.iso_currency
                          WHERE gi.granularity_level_id = 58 --AND gi.id = '313440'
                          AND cm.[iso_currency] NOT IN ('BOV', 'CLF', 'COU', 'CUC', 'GBX', 'ILX', 'UYW', 'UYI', 'ZWL', '999')
                          AND cm.[active] = '1' 
                          ORDER BY data_timestamp DESC



SELECT TOP 1 [date] 
FROM Factset.ref_v2.fx_rates_usd--staging.dbo.indx_index_data 
--WHERE index_id = 4884 
ORDER BY [date]