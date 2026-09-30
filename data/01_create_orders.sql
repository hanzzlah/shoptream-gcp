CREATE SCHEMA IF NOT EXISTS shopstream;

DROP TABLE IF EXISTS shopstream.orders;

CREATE TABLE shopstream.orders (
    order_id     VARCHAR(36) PRIMARY KEY,
    customer_id  VARCHAR(36) NOT NULL,
    product_id   VARCHAR(36) NOT NULL,
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    order_status VARCHAR(20) NOT NULL CHECK (
        order_status IN ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED')
    ),
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL,
    region       VARCHAR(20) NOT NULL
);
