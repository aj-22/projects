
## MYSQL
```sh
mysql -u root -h localhost -p
```
Create table in MYSQL
```sql
CREATE DATABASE sales_db;
USE DATABASE sales_db;
CREATE TABLE total_sales_by_source_state (
    source varchar(100),
    state varchar(100),
    total_sum_amount double,
    processed_at datetime,
    batch_id int(11)
)
```


Create table in Cassandra

```SQL
create keyspace sales_ks with replication={'class':'SimpleStrategy','replication_factor':1}
```
```SQL
CREATE TABLE sales_ks.orders_tbl (
Row_ID  int primary key, Order_ID  text, Order_Date text , Ship_Date  text , Ship_Mode  text ,Customer_ID  text, Customer_Name text, Segment text, "country region"  text, City  text, State  text, Postal_Code  int, Region  text, Product_ID  text , Category  text , Sub_Category  text , Product_Name  text, Sales  double, Quantity int, Discount  double, Profit  double, timestamp text )
```

```SQL
create table sales_ks.orders_tbl (order_id int primary key, created_at text, discount text, product_id text, quantity text, subtotal text, tax text, total text, customer_id int, timestamp text )

```
