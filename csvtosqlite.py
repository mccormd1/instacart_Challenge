import sqlite3
import csv
conn = sqlite3.connect('instacart2.sqlite')
cur=conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS aisles;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_products__train;
DROP TABLE IF EXISTS order_products__prior;

CREATE TABLE aisles (
aisle_id INTEGER PRIMARY KEY,
aisle TEXT
);

CREATE TABLE departments (
department_id INTEGER PRIMARY KEY,
department TEXT
);

CREATE TABLE products (
product_id INTEGER PRIMARY KEY,
product_name TEXT,
aisle_id INTEGER,
department_id INTEGER
);

CREATE TABLE orders (
order_id INTEGER PRIMARY KEY,
user_id INTEGER,
eval_set TEXT,
order_number INTEGER,
order_dow INTEGER,
order_hour_of_day INTEGER,
days_since_prior_order TEXT
);

CREATE TABLE order_products__prior (
order_id INTEGER ,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER
);

CREATE TABLE order_products__train (
order_id INTEGER ,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER
);
''')

countmod=1000000
fname = 'aisles.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        aisle=line[1].strip()
#         print(id,aisle)
        cur.execute('''INSERT INTO aisles (aisle_id,aisle)
                VALUES( ?, ? )''', (id,aisle))
                
        if count%countmod == 0:
            conn.commit()
        count+=1
        
conn.commit()
fname = 'departments.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        department=line[1].strip()
#         print(id,department)
        cur.execute('''INSERT INTO departments (department_id,department)
                VALUES( ?, ? )''', (id,department))
                
        if count%countmod == 0:
            conn.commit()
        count+=1

conn.commit()        

fname = 'products.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        product=line[1].strip()
        aisle=int(line[2].strip())
        department=int(line[3].strip())
#         print(id,aisle)
        cur.execute('''INSERT INTO products (product_id,product_name,aisle_id,department_id)
                VALUES( ?, ?, ?, ? )''', (id,product,aisle,department))
                
        if count%countmod == 0:
            conn.commit()
        count+=1

conn.commit()
        
fname = 'orders.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        user=int(line[1].strip())
        eval=line[2].strip()
        number=int(line[3].strip())
        dow=int(line[4].strip())
        hour=int(line[5].strip())
        days=line[6].strip()
#         print(id,aisle)
        cur.execute('''INSERT INTO orders (order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order)
                VALUES( ?, ?, ?, ?, ?, ?, ? )''', (id,user,eval,number,dow,hour,days))
                
        if count%countmod == 0: #only commit after collecting countmod number of rows
            conn.commit()
        count+=1

conn.commit() #commits last chunk if there are any leftovers

fname = 'order_products__prior.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        product=int(line[1].strip())
        addorder=int(line[2].strip())
        reorder=int(line[3].strip())
#         print(id,aisle)
        cur.execute('''INSERT INTO order_products__prior (order_id,product_id,add_to_cart_order,reordered)
                VALUES( ?, ?, ?, ? )''', (id,product,addorder,reorder))
                
        if count%countmod == 0: #only commit after collecting countmod number of rows
            conn.commit()
        count+=1

conn.commit() #commits last chunk if there are any leftovers

fname = 'order_products__train.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    for line in fh:
#         print(line[0].strip())
        id=int(line[0].strip())
        product=int(line[1].strip())
        addorder=int(line[2].strip())
        reorder=int(line[3].strip())
#         print(id,aisle)
        cur.execute('''INSERT INTO order_products__train (order_id,product_id,add_to_cart_order,reordered)
                VALUES( ?, ?, ?, ? )''', (id,product,addorder,reorder))
                
        if count%countmod == 0: #only commit after collecting countmod number of rows
            conn.commit()
        count+=1

conn.commit() #commits last chunk if there are any leftovers
cur.close()