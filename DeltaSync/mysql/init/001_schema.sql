CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'deltasync-debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'%';

USE deltasync;

CREATE TABLE customers (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email VARCHAR(320) NOT NULL,
  full_name VARCHAR(200) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uq_customers_email (email)
) ENGINE=InnoDB;

CREATE TABLE orders (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id BIGINT UNSIGNED NOT NULL,
  order_date DATE NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  total_amount DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY ix_orders_customer_id (customer_id),
  KEY ix_orders_order_date (order_date),
  CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
) ENGINE=InnoDB;

INSERT INTO customers (id, email, full_name, status) VALUES
  (1, 'ada@example.com', 'Ada Lovelace', 'active'),
  (2, 'grace@example.com', 'Grace Hopper', 'active'),
  (3, 'delete-me@example.com', 'Delete Example', 'inactive');

INSERT INTO orders (id, customer_id, order_date, status, total_amount) VALUES
  (1001, 1, '2026-09-22', 'paid', 125.50),
  (1002, 2, '2026-09-23', 'pending', 89.99);
