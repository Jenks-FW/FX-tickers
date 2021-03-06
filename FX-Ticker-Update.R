##----------------- Header ------------------------------------------
## Script name: Global Currency Exchange Rate Tickers
## Purpose: Update FX tickers (daily)
## Author: Brad Jenkins
## Date Created: 2020-01-02
## Email: BJenkins@FreightWaves.com
##--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
## Notes: Append new Forex data to 
##                              Staging.dbo.indx_index_data 
##                              from 
##                              Factset.ref_v2.fx_rates_usd
##
##--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---


## --------------- Initialize ---------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, DBI, lubridate, odbc, magrittr)


## --------------- SQL Connection -----------------------------------

#Connect to SQL Server
sql_connect <- function(database, user = 'fwetl'){
  db <- case_when(tolower(database) %in% c('s', 'staging', 'stage') ~ "Staging",
                  tolower(database) %in% c('w', 'warehouse', 'ware', 'wh') ~ "Warehouse",
                  tolower(database) %in% c('f','factset','fs') ~ "Factset",
                  tolower(database) %in% c('segment', 'seg', 'user') ~ "Segment")
  if(is.na(db)) {
    return('You must choose either Staging, Warehouse, Segment or Factset.')
    break
  } else {
    return(odbc::dbConnect(odbc(),
                           Driver = "SQL Server",
                           Server = "freightwaves.ctaqnedkuefm.us-east-2.rds.amazonaws.com",
                           Database = db,
                           UID = Sys.getenv("user"),
                           PWD = Sys.getenv("pass"),
                           Port = 1433))
  }
}

fcon <- sql_connect('f')
scon <- sql_connect('s')


## --------------- Load in staging data -----------------------------

granularity_index <- 
  tbl(scon, 'indx_granularity_item') %>% 
  filter(granularity_level_id == 58) %>% # pull only forex granularities
  select(granularity_item_id = id, 
         granularity_level_id, 
         granularity1, 
         Description) %>% 
  collect()  

# Only need last date this updated in staging
mxdt <- dbGetQuery(scon, 'SELECT TOP 1 data_timestamp 
                          FROM staging.dbo.indx_index_data 
                          WHERE index_id = 4884 
                          ORDER BY data_timestamp desc')

mxdt <- ymd(mxdt[1,1]) 
#mxdt <- ymd('2020-02-02') # set a date to troubleshoot

# Make sure there's a date value
if (is.na(mxdt)) {
  mxdt <- ymd('1960-01-01')
}


## --------------- Load in factset data -----------------------------

fx_tmp <- 
  tbl(fcon, dbplyr::in_schema('ref_v2', 'fx_rates_usd')) %>%
  filter(!iso_currency %in% c('BOV', 'CLF', 'COU', 'CUC', 'GBX', 
                              'ILX', 'UYW', 'UYI', 'ZWL', '999'),
         date > local(mxdt[1])) %>%  # only grab data newer than mxdt
  select(iso_currency, date, exch_rate_usd, exch_rate_per_usd) %>% 
  collect() %>%  
  mutate(to_usd = paste0(.$iso_currency, 'USD'), 
         per_usd = paste0('USD', .$iso_currency)) %>% 
  select(iso_currency, data_timestamp = date, 
         to_usd, exch_rate_usd, 
         per_usd, exch_rate_per_usd) %>% 
  mutate(data_timestamp = ymd(data_timestamp))


## --------------- Prepare forex data -------------------------------

# to_usd example: EURUSD is the number of US Dollars one Euro will buy.
fx_to_usd <-  #unique(fx_to_usd$granularity1) # 189 unique granularities
  fx_tmp %>% select(granularity1 = to_usd, 
                    data_timestamp, 
                    data_value = exch_rate_usd)

# per_usd example: USDEUR is the number of Euros one US Dollar will buy. 
fx_per_usd <-  #unique(fx_per_usd$granularity1) # 189 unique granularities
  fx_tmp %>% select(granularity1 = per_usd, 
                    data_timestamp, 
                    data_value = exch_rate_per_usd)


## --------------- Match up with Staging ----------------------------

fx_new <-  
  bind_rows(fx_to_usd, fx_per_usd) %>% 
  inner_join(., granularity_index, by = 'granularity1') %>%
  mutate(index_id = 4884L) %>% 
  select(index_id, granularity_item_id, data_timestamp, data_value)#, granularity1)

# 302 unique granularities after joining to granularity_index
#unique(fx_new$granularity_item_id)
#bind_rows(fx_to_usd, fx_per_usd) %>% 
#  anti_join(., granularity_index, by = 'granularity1') %>% 
#  distinct(granularity1)


## --------------- Validation ---------------------------------------
# Make a new row of data to see if it is only thing assigned to forex_new
#fx_index[nrow(fx_index) + 1,] <- 
#  list(4884L, 313000L, ymd('2020-01-06'), as.double(.000001))
# Make sure everything looks right and col classes are maintained
#glimpse(fx_index) 

# Assign only new data to fx_2
#fx_2 <- anti_join(fx_index, 
#                  fx_new, 
#                  by = c('granularity_item_id', 'data_timestamp'))
# Look and see how it turned out, col classes look good
#glimpse(fx_new) # Check if only thing in here is the new line


## --------------- Export -------------------------------------------
dbWriteTable(scon, 
             "indx_index_data", 
             fx_new,
             overwrite = F,
             append = T)


## --------------- Granularities ------------------------------------
#Granularities <- fx_new$granularity1
#Currencies <- str_extract_all(fx_new$granularity1[1:151], pattern = "^[A-Z]{3}")
#currency_map <- 
#  tbl(fcon, dbplyr::in_schema('ref_v2', 'iso_currency_map')) %>%
#  select(iso_currency, currency_desc) %>% 
#  collect()
#currency_map %<>% filter(iso_currency %in% Currencies) 
#write_csv(currency_map, path = "C:/Users/bjenkins/Documents/RStudio/FX-Tickers/currency_map.csv")

          