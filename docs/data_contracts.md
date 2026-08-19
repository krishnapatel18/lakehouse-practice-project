# Data Contract 
## 1. Customers 
- Purpose: Stores customer master data. 
- Grain: One row = One customer details record 
- Business key: `customer_id` 
- Source table: `catalog_project1.source1.customers_raw`
- Source change behavior: Records are inserted, updated over time 
- Historical requirement: No business history required 
- Bronze: Append only 
- Silver: Latest record only 
- Gold: `dim_customers` with SCD1 

## 2. Products 
- Purpose: Stores products master data. 
- Grain: One row = One product details record 
- Business key: `product_id` 
- Source table: `catalog_project1.source1.products_raw`
- Source change behavior: Records are inserted, updated over time 
- Historical requirement: Product price at different points in time  
- Bronze: Append only 
- Silver: Latest record only 
- Gold: `dim_products` with SCD2 

## 3. Inventory  
- Purpose: Stores inventory master data. 
- Grain: One row = One inventory record 
- Business key: inventory_id 
- Source table: `catalog_project1.source1.inventory_raw`
- Source change behavior: Records are inserted, updated over time 
- Historical requirement: Business history required 
- Bronze: Append only 
- Silver: Latest record only 
- Gold: `inventory_snapshot` with SCD1 

## 4. Orders  
- Purpose: Stores orders transactional data. 
- Grain: One row = One order details record 
- Business key: `order_id` 
- Source table: `catalog_project1.source1.orders_raw`
- Source change behavior: Records are inserted, updated over time 
- Historical requirement: Order status at perticular time 
- Bronze: Append only 
- Silver: Latest record only 
- Gold: `fact_order_status_history` with SCD2 

## 5. Payments 
- Purpose: Stores payment transactional data. 
- Grain: One row = One payment record 
- Business key: `payment_id` 
- Source table: `catalog_project1.source1.payments_raw`
- Source change behavior: Records are inserted, updated over time 
- Historical requirement: No business history required 
- Bronze: Append only 
- Silver: Latest record only 
- Gold: `payment_analytics` with SCD1 

## 6. Order Items  
- Purpose: Stores order items transactional data. 
- Grain: One row = One product details in one order  
- Business key: `order_item_id` 
- Source table: `catalog_project1.source1.order_items_raw`
- Source change behavior: Records are inserted 
- Historical requirement: No business history required 
- Bronze: Append only 
- Silver: Immutable  
- Gold: `fact_sales` 

