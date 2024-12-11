-- Databricks notebook source
drop database if EXISTS dev.retail_store cascade;

create DATABASE if not EXISTS dev.retail_store;

create table if not exists dev.retail_store.invoices(
  invoice_number string,
  created_time bigint,
  store_id string,
  pos_id string,
  customer_type string,
  payment_method string,
  Delivery_type string,
  city string,
  state string,
  pincode string,
  item_code string,
  item_description string,
  item_price double,
  item_qty int,
  total_value double
) using delta;


create external volume if not exists dev.retail_store.landing_zone
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/retail_store_app/landing_zone';

create external volume if not exists dev.retail_store.invoices_archive
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/retail_store_app/invoices_archive'; 

create external volume if not exists dev.retail_store.checkpoint_invoices_raw
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/retail_store_app/checkpoint_invoices_raw';

create external volume if not exists dev.retail_store.checkpoint_invoices
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/retail_store_app/checkpoint_invoices';

create external volume if not exists dev.retail_store.checkpoint_customer_revenue
location 'abfss://structured-streaming-course@dbstorageact.dfs.core.windows.net/retail_store_app/checkpoint_customer_revenue';

-- COMMAND ----------

create table DEV.retail_store.customer_revenue (
  CustomerCardNo string,
  total_amount double,
  rewards double
) using delta;

-- COMMAND ----------

CREATE TABLE dev.retail_store.invoices_raw (
  key STRING,
  value STRUCT<InvoiceNumber: STRING, CreatedTime: BIGINT, StoreID: STRING, PosID: STRING, CashierID: STRING, CustomerType: STRING, CustomerCardNo: INT, TotalAmount: DOUBLE, NumberOfItems: INT, PaymentMethod: STRING, TaxableAmount: DOUBLE, CGST: DOUBLE, SGST: DOUBLE, CESS: DOUBLE, DeliveryType: STRING, DeliveryAddress: STRUCT<AddressLine: STRING, City: STRING, State: STRING, PinCode: STRING, ContactNumber: STRING>, InvoiceLineItems: ARRAY<STRUCT<ItemCode: STRING, ItemDescription: STRING, ItemPrice: DOUBLE, ItemQty: INT, TotalValue: DOUBLE>>>,
  topic STRING,
  timestamp TIMESTAMP)
USING delta;

-- COMMAND ----------

CREATE TABLE dev.retail_store.invoices_raw (
  InvoiceNumber STRING,
  CreatedTime BIGINT,
  StoreID STRING,
  PosID STRING,
  CashierID STRING,
  CustomerType STRING,
  CustomerCardNo INT,
  TotalAmount DOUBLE,
  NumberOfItems INT,
  PaymentMethod STRING,
  TaxableAmount DOUBLE,
  CGST DOUBLE,
  SGST DOUBLE,
  CESS DOUBLE,
  DeliveryType STRING,
  DeliveryAddress STRUCT<AddressLine: STRING, City: STRING, State: STRING, PinCode: STRING, ContactNumber: STRING>,
  InvoiceLineItems ARRAY<STRUCT<ItemCode: STRING, ItemDescription: STRING, ItemPrice: DOUBLE, ItemQty: INT, TotalValue: DOUBLE>>)
 USING delta;

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/dev/retail_store/landing_zone

-- COMMAND ----------

select * from dev.retail_store.invoices;
