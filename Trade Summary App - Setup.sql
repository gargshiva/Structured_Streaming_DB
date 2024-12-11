-- Databricks notebook source
drop database if exists dev.finance cascade;

create database if not exists dev.finance;

-- Bronze Layer table with raw data
create table if not exists dev.finance.trades_raw(
key string,
value string
) using delta;

INSERT INTO dev.finance.trades_raw VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:05:00", "Type": "BUY", "Amount": 500, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:12:00", "Type": "BUY", "Amount": 300, "BrokerCode": "ABX"}');

INSERT INTO dev.finance.trades_raw VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:20:00", "Type": "BUY", "Amount": 600, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:40:00", "Type": "BUY", "Amount": 900, "BrokerCode": "ABX"}');

INSERT INTO dev.finance.trades_raw VALUES
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:48:00", "Type": "SELL", "Amount": 500, "BrokerCode": "ABX"}'),
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:25:00", "Type": "SELL", "Amount": 400, "BrokerCode": "ABX"}');

create external volume if not exists dev.finance.trades_raw_checkpoint
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/trade_summary_app/trades_raw_checkpoint';

create external volume if not exists dev.finance.trades_summary_checkpoint
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/trade_summary_app/trades_summary_checkpoint';

-- COMMAND ----------

select * from dev.finance.trades_raw;
