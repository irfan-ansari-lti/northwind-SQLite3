from datetime import timedelta, datetime
from random import randint
from random import choice as rc
import snowflake.connector
import os

snowflake.connector.paramstyle='qmark'
database='demo_db'
schema='ss'

# This function will return a random datetime between two datetime objects.
def random_date(start, end):
  return start + timedelta(seconds=randint(0, int((end - start).total_seconds())))

# Connect to the Snowflake DB
conn = snowflake.connector.connect(
        account='intinfotech.ap-southeast-2',
        user=os.environ.get('northwind-snowflake-username'),
        password=os.environ.get('northwind-snowflake-password'),
        database='demo_db',
        schema='ss',
        warehouse='demo_wh'
      )
c = conn.cursor()

# ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode
c.execute(f"select distinct ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry from {database}.{schema}.orders")
locations = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in c.fetchall()]

# Customer.Id
c.execute(f"select distinct employeeid from {database}.{schema}.employees")
employees = [row[0] for row in c.fetchall()]

# Shipper.Id
c.execute(f"select distinct shipperid from {database}.{schema}.shippers")
shippers = [row[0] for row in c.fetchall()]

# Customer.Id
c.execute(f"select distinct customerid from {database}.{schema}.customers")
customers = [row[0] for row in c.fetchall()]

# Create a bunch of new orders
sql = f'INSERT INTO {database}.{schema}.orders (OrderId, CustomerId, EmployeeId, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
max_orderid = c.execute(f'select max(orderid) from {database}.{schema}.orders').fetchone()[0]
params_list = []

n_new_orders = randint(10, 50)
print(f'Generating {n_new_orders} new orders...')
for order_id, i in enumerate(range(n_new_orders), start=max_orderid+1):
  location = rc(locations)
  order_date = random_date(datetime.strptime('2012-07-10', '%Y-%m-%d'), datetime.today())
  required_date = random_date(order_date, order_date+timedelta(days=randint(14,60)))
  shipped_date = random_date(order_date, order_date+timedelta(days=randint(1,30)))
  params = (
    order_id,
    rc(customers),  # CustomerId
    rc(employees),  # EmployeeId
    order_date,     # OrderDate
    required_date,  # RequiredDate
    shipped_date,   # ShippedDate
    rc(shippers),   # ShipVia
    0.00,           # Freight
    location[0],    # ShipName
    location[1],    # ShipAddress
    location[2],    # ShipCity
    location[3],    # ShipRegion
    location[4],    # ShipPostalCode
    location[5],    # ShipCountry
  )
  params_list.append(params)

c.executemany(sql, params_list)

# Product.Id
c.execute(f"select distinct productid, UnitPrice from {database}.{schema}.products")
products = [(row[0], row[1]) for row in c.fetchall()]

# Order.Id
c.execute(f"select distinct orderid from {database}.{schema}.orders where Freight = 0.00 and orderid > {max_orderid} - 122")
orders = [row[0] for row in c.fetchall()]

# Fill the order with items
params_list = []
for order in orders:
  used = []
  sql = f'INSERT INTO {database}.{schema}.order_details (OrderId, ProductId, UnitPrice, Quantity, Discount) VALUES (?, ?, ?, ?, ?)'
  for x in range(randint(1,len(products))):
    control = 1
    while control:
      product = rc(products)
      if product not in used:
        used.append(product)
        control = 0
    params = (
      order,          # OrderId
      product[0],     # ProductId
      product[1],     # UnitPrice
      randint(1,50),  # Quantity
      0,              # Discount
    )
    params_list.append(params)

c.executemany(sql, params_list)

# Cleanup
# c.execute('update [Order] set OrderDate = date(OrderDate), RequiredDate = date(RequiredDate), ShippedDate = date(ShippedDate)')
c.execute(f"select sum(Quantity)*0.25+10, OrderId from {database}.{schema}.order_details group by OrderId")
orders = [(row[0],row[1]) for row in c.fetchall()]
for order in orders:
  c.execute(f"update {database}.{schema}.orders set Freight=? where OrderId=?", (order[0], order[1]))

conn.commit()
conn.close()