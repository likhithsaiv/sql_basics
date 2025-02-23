import sqlite3

# Create a connection to the SQLite database
connection = sqlite3.connect('example.db')
cursor = connection.cursor()

# --- Table Creation ---
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    email TEXT NOT NULL UNIQUE,
    city TEXT  -- Added city column
)
''')

# --- Data Insertion ---
# Using parameterized queries (more secure and efficient)
users_data = [
    ('Alice', 30, 'alice@example.com', 'New York'),
    ('Bob', 25, 'bob@example.com', 'Los Angeles'),
    ('Charlie', 35, 'charlie@example.com', 'Chicago'),
    ('David', 40, 'david@example.com', 'Houston') # Added a new user
]
cursor.executemany('INSERT INTO users (name, age, email, city) VALUES (?, ?, ?, ?)', users_data)
connection.commit()

# --- Querying Data ---
print("Users in the database:")
cursor.execute('SELECT * FROM users')  # Select all columns
for row in cursor.fetchall():
    print(row)

# --- Specific Queries ---

# Selecting specific columns
print("\nUser Names and Ages:")
cursor.execute('SELECT name, age FROM users')
for row in cursor.fetchall():
    print(row)

# Filtering with WHERE clause
print("\nUsers older than 30:")
cursor.execute('SELECT * FROM users WHERE age > 30')
for row in cursor.fetchall():
    print(row)

# Using LIKE for pattern matching
print("\nUsers with email ending with '@example.com':")
cursor.execute("SELECT * FROM users WHERE email LIKE '%@example.com'")
for row in cursor.fetchall():
    print(row)

# Using IN to check for multiple values
print("\nUsers in New York or Chicago:")
cursor.execute("SELECT * FROM users WHERE city IN ('New York', 'Chicago')")
for row in cursor.fetchall():
    print(row)


# Using ORDER BY to sort results
print("\nUsers sorted by age (ascending):")
cursor.execute('SELECT * FROM users ORDER BY age ASC')
for row in cursor.fetchall():
    print(row)

print("\nUsers sorted by name (descending):")
cursor.execute('SELECT * FROM users ORDER BY name DESC')
for row in cursor.fetchall():
    print(row)


# Using LIMIT to restrict the number of results
print("\nFirst 2 users:")
cursor.execute('SELECT * FROM users LIMIT 2')
for row in cursor.fetchall():
    print(row)


# --- Updating Data ---
cursor.execute('UPDATE users SET age = 31 WHERE name = ?', ('Alice',))  # Parameterized update
connection.commit()

# --- Deleting Data ---
cursor.execute('DELETE FROM users WHERE name = ?', ('Bob',)) # Parameterized delete
connection.commit()

# --- Displaying Updated Data ---
print("\nUpdated users in the database:")
cursor.execute('SELECT * FROM users')
for row in cursor.fetchall():
    print(row)

# --- Orders Table ---
cursor.execute('''
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
''')

orders_data = [
    (1, 'Laptop', 1),
    (1, 'Mouse', 2),
    (3, 'Keyboard', 1),
    (4, 'Monitor', 2)  # Order for the new user (David)
]
cursor.executemany('INSERT INTO orders (user_id, product_name, quantity) VALUES (?, ?, ?)', orders_data)
connection.commit()

print("\nOrders in the database:")
cursor.execute('SELECT * FROM orders')
for row in cursor.fetchall():
    print(row)

# --- Joining Tables ---
# Get user information along with their orders
print("\nUser orders (JOIN):")
cursor.execute('''
SELECT users.name, orders.product_name, orders.quantity
FROM users
INNER JOIN orders ON users.id = orders.user_id
''') # Explicitly specify INNER JOIN
for row in cursor.fetchall():
    print(row)

# --- Views ---
# Create a view to simplify querying user orders
cursor.execute('''
CREATE VIEW IF NOT EXISTS user_orders_view AS
SELECT users.name, orders.product_name, orders.quantity
FROM users
INNER JOIN orders ON users.id = orders.user_id
''')

# Query the view
print("\nUser orders (from view):")
cursor.execute('SELECT * FROM user_orders_view')
for row in cursor.fetchall():
    print(row)


# --- Aggregate Functions ---

# Count the number of users
cursor.execute('SELECT COUNT(*) FROM users')
user_count = cursor.fetchone()[0]  # Fetch the single value
print(f"\nTotal number of users: {user_count}")

# Calculate the average age of users
cursor.execute('SELECT AVG(age) FROM users')
average_age = cursor.fetchone()[0]
print(f"Average age of users: {average_age}")

# Find the maximum age
cursor.execute("SELECT MAX(age) from users")
max_age = cursor.fetchone()[0]
print(f"Maximum age: {max_age}")

# Sum of quantities in orders
cursor.execute("SELECT SUM(quantity) FROM orders")
total_quantity = cursor.fetchone()[0]
print(f"Total quantity of all orders: {total_quantity}")


# --- GROUP BY and HAVING ---

# Count the number of orders for each user
print("\nNumber of orders per user:")
cursor.execute('''
    SELECT u.name, COUNT(o.order_id) AS order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
''') # Use LEFT JOIN to include users with no orders
for row in cursor.fetchall():
    print(row)

# Find users who have placed more than 1 order
print("\nUsers with more than 1 order:")
cursor.execute('''
    SELECT u.name, COUNT(o.order_id) AS order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
    HAVING COUNT(o.order_id) > 1
''')
for row in cursor.fetchall():
    print(row)


# --- Transactions ---

# Example of using a transaction (all or nothing)
try:
    # Start a transaction
    connection.execute('BEGIN')

    # Insert a new user
    cursor.execute("INSERT INTO users (name, age, email, city) VALUES (?, ?, ?, ?)", ('Eve', 28, 'eve@newdomain.com', 'Miami'))

    # Insert an order for the new user (but with an invalid user_id)
    # This will cause an error (foreign key constraint violation)
    cursor.execute("INSERT INTO orders (user_id, product_name, quantity) VALUES (?, ?, ?)", (999, 'Tablet', 1)) # Invalid user_id


    # Commit the transaction (this will not happen due to the error)
    connection.commit()
    print("Transaction committed successfully.")

except sqlite3.IntegrityError as e:
    # Rollback the transaction if there's an error
    connection.rollback()
    print(f"Transaction rolled back due to error: {e}")

except Exception as e:
    connection.rollback()
    print(f"An unexpected error occurred: {e}")

finally:
     #Check users table
    print("\nUsers after (possible) failed transaction:")
    cursor.execute('SELECT * from users')
    for user in cursor.fetchall():
        print(user)

    # Check orders table
    print("\nOrders after (possible) failed transaction:")
    cursor.execute("SELECT * FROM orders")
    for order in cursor.fetchall():
        print(order)

# --- Triggers (SQLite Limitation) ---

# SQLite does *not* support stored procedures or user-defined functions
# in the same way as more advanced database systems like PostgreSQL or MySQL.
#  The `CREATE FUNCTION` syntax in the original code is for PostgreSQL (plpgsql),
#  and it will *not* work in SQLite.  Also trigger needs to be simplified for sqlite

# Simplified Trigger example (for demonstration purposes - limited functionality)
cursor.execute('''
CREATE TRIGGER IF NOT EXISTS update_user_age
AFTER UPDATE OF age ON users
FOR EACH ROW
BEGIN
    UPDATE users SET age = NEW.age WHERE id = OLD.id;
END;
''')



# --- Closing Connection ---
cursor.close()
connection.close()