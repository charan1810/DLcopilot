\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS src CASCADE;
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA src;
CREATE SCHEMA IF NOT EXISTS "CORE";
SET search_path TO src, public;

CREATE TABLE categories (
    category_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_category_id INTEGER REFERENCES categories(category_id),
    category_name VARCHAR(120) NOT NULL UNIQUE,
    category_slug VARCHAR(120) NOT NULL UNIQUE,
    department_name VARCHAR(80) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATE NOT NULL
);

CREATE TABLE brands (
    brand_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    brand_name VARCHAR(160) NOT NULL UNIQUE,
    country_code CHAR(2) NOT NULL,
    founded_year INTEGER NOT NULL,
    premium_flag BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE suppliers (
    supplier_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_name VARCHAR(180) NOT NULL UNIQUE,
    supplier_tier VARCHAR(20) NOT NULL,
    country_code CHAR(2) NOT NULL,
    lead_time_days INTEGER NOT NULL,
    active_since DATE NOT NULL
);

CREATE TABLE warehouses (
    warehouse_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    warehouse_code VARCHAR(20) NOT NULL UNIQUE,
    warehouse_name VARCHAR(160) NOT NULL,
    region_name VARCHAR(80) NOT NULL,
    timezone_name VARCHAR(80) NOT NULL,
    capacity_units INTEGER NOT NULL,
    opened_date DATE NOT NULL
);

CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_number VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(80) NOT NULL,
    last_name VARCHAR(80) NOT NULL,
    email VARCHAR(180) NOT NULL UNIQUE,
    phone VARCHAR(30) NOT NULL,
    date_of_birth DATE,
    gender VARCHAR(20),
    signup_date DATE NOT NULL,
    acquisition_channel VARCHAR(40) NOT NULL,
    loyalty_tier VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    marketing_opt_in BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE customer_addresses (
    address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    address_type VARCHAR(20) NOT NULL,
    line1 VARCHAR(180) NOT NULL,
    city VARCHAR(80) NOT NULL,
    state_code VARCHAR(20) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country_code CHAR(2) NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku VARCHAR(30) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(category_id),
    brand_id INTEGER NOT NULL REFERENCES brands(brand_id),
    color_name VARCHAR(40) NOT NULL,
    size_label VARCHAR(20) NOT NULL,
    unit_cost NUMERIC(10, 2) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    launch_date DATE NOT NULL,
    average_rating NUMERIC(3, 2) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE product_suppliers (
    product_supplier_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    supplier_id INTEGER NOT NULL REFERENCES suppliers(supplier_id),
    supplier_sku VARCHAR(40) NOT NULL,
    supply_cost NUMERIC(10, 2) NOT NULL,
    lead_time_days INTEGER NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from DATE NOT NULL,
    UNIQUE (product_id, supplier_id)
);

CREATE TABLE inventory_snapshots (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(warehouse_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    on_hand_qty INTEGER NOT NULL,
    reserved_qty INTEGER NOT NULL,
    reorder_point_qty INTEGER NOT NULL,
    incoming_qty INTEGER NOT NULL,
    UNIQUE (snapshot_date, warehouse_id, product_id)
);

CREATE TABLE orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_number VARCHAR(24) NOT NULL UNIQUE,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    billing_address_id BIGINT NOT NULL REFERENCES customer_addresses(address_id),
    shipping_address_id BIGINT NOT NULL REFERENCES customer_addresses(address_id),
    order_ts TIMESTAMP NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    sales_channel VARCHAR(30) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    subtotal_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    shipping_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    payment_status VARCHAR(20) NOT NULL,
    fulfilled_ts TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(warehouse_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    discount_amount NUMERIC(10, 2) NOT NULL,
    gross_amount NUMERIC(12, 2) NOT NULL,
    net_amount NUMERIC(12, 2) NOT NULL,
    item_status VARCHAR(20) NOT NULL
);

CREATE TABLE payments (
    payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE REFERENCES orders(order_id),
    payment_ts TIMESTAMP NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    captured_amount NUMERIC(12, 2) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    gateway_name VARCHAR(40) NOT NULL,
    authorization_code VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE shipments (
    shipment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE REFERENCES orders(order_id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(warehouse_id),
    carrier_name VARCHAR(40) NOT NULL,
    tracking_number VARCHAR(30) NOT NULL UNIQUE,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    shipping_service VARCHAR(30) NOT NULL,
    shipment_status VARCHAR(20) NOT NULL,
    freight_cost NUMERIC(10, 2) NOT NULL
);

CREATE TABLE reviews (
    review_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    rating INTEGER NOT NULL,
    review_title VARCHAR(120) NOT NULL,
    review_body TEXT NOT NULL,
    review_date DATE NOT NULL,
    is_verified_purchase BOOLEAN NOT NULL DEFAULT TRUE,
    helpful_votes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE returns (
    return_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_item_id BIGINT NOT NULL UNIQUE REFERENCES order_items(order_item_id),
    return_ts TIMESTAMP NOT NULL,
    return_reason VARCHAR(40) NOT NULL,
    return_status VARCHAR(20) NOT NULL,
    refund_amount NUMERIC(10, 2) NOT NULL,
    restockable BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_customers_signup_date ON customers(signup_date);
CREATE INDEX idx_customer_addresses_customer_id ON customer_addresses(customer_id);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_brand_id ON products(brand_id);
CREATE INDEX idx_inventory_snapshots_product_warehouse ON inventory_snapshots(product_id, warehouse_id);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_ts ON orders(order_ts);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_reviews_product_id ON reviews(product_id);
CREATE INDEX idx_returns_return_ts ON returns(return_ts);

INSERT INTO categories (
    parent_category_id,
    category_name,
    category_slug,
    department_name,
    is_active,
    created_at
)
WITH seed AS (
    SELECT ARRAY[
        'Mobile Phones', 'Laptops', 'Tablets', 'Smart Home', 'Audio',
        'TV & Video', 'Kitchen Appliances', 'Home Decor', 'Furniture', 'Lighting',
        'Mens Apparel', 'Womens Apparel', 'Footwear', 'Accessories', 'Beauty',
        'Health & Wellness', 'Fitness Equipment', 'Outdoor Gear', 'Pet Supplies', 'Baby Essentials',
        'Books', 'Stationery', 'Toys', 'Gaming', 'Automotive',
        'Garden', 'Travel', 'Office Furniture', 'Sports Nutrition', 'Seasonal Deals'
    ]::text[] AS category_names,
    ARRAY[
        'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics',
        'Electronics', 'Home', 'Home', 'Home', 'Home',
        'Fashion', 'Fashion', 'Fashion', 'Fashion', 'Beauty',
        'Health', 'Sports', 'Sports', 'Lifestyle', 'Lifestyle',
        'Media', 'Media', 'Kids', 'Gaming', 'Automotive',
        'Home', 'Lifestyle', 'Office', 'Health', 'Promotions'
    ]::text[] AS departments
)
SELECT
    CASE WHEN gs <= 8 THEN NULL ELSE ((gs - 1) % 8) + 1 END,
    seed.category_names[gs],
    lower(replace(seed.category_names[gs], ' ', '-')),
    seed.departments[gs],
    TRUE,
    CURRENT_DATE - ((gs * 11) % 1800)
FROM generate_series(1, 12) AS gs
CROSS JOIN seed;

INSERT INTO brands (
    brand_name,
    country_code,
    founded_year,
    premium_flag
)
WITH seed AS (
    SELECT ARRAY['North', 'Blue', 'Urban', 'Atlas', 'Silver', 'Summit', 'Nimbus', 'Prime', 'Terra', 'Pulse', 'Nova', 'Vertex']::text[] AS roots,
           ARRAY['Works', 'Collective', 'Labs', 'Supply', 'Studio', 'House', 'Gear', 'Line', 'Factory', 'Market']::text[] AS suffixes,
           ARRAY['US', 'CA', 'DE', 'JP', 'KR', 'IN', 'GB', 'FR', 'SE', 'AU']::text[] AS countries
)
SELECT
    format('%s %s %s',
        seed.roots[((gs - 1) % array_length(seed.roots, 1)) + 1],
        seed.suffixes[(((gs - 1) / array_length(seed.roots, 1)) % array_length(seed.suffixes, 1)) + 1],
        lpad(gs::text, 3, '0')
    ),
    seed.countries[((gs - 1) % array_length(seed.countries, 1)) + 1],
    1985 + (gs % 35),
    (gs % 7 = 0)
FROM generate_series(1, 18) AS gs
CROSS JOIN seed;

INSERT INTO suppliers (
    supplier_name,
    supplier_tier,
    country_code,
    lead_time_days,
    active_since
)
WITH seed AS (
    SELECT ARRAY['Acme', 'Mercury', 'Sterling', 'Keystone', 'Evergreen', 'Pioneer', 'Crown', 'Harbor', 'Vertex', 'Cobalt']::text[] AS roots,
           ARRAY['Distribution', 'Wholesale', 'Logistics', 'Imports', 'Trading']::text[] AS suffixes,
           ARRAY['US', 'CN', 'VN', 'IN', 'MX', 'PL', 'DE', 'TR', 'TH', 'MY']::text[] AS countries
)
SELECT
    format('%s %s %s',
        seed.roots[((gs - 1) % array_length(seed.roots, 1)) + 1],
        seed.suffixes[(((gs - 1) / array_length(seed.roots, 1)) % array_length(seed.suffixes, 1)) + 1],
        lpad(gs::text, 3, '0')
    ),
    CASE WHEN gs % 10 = 0 THEN 'strategic' WHEN gs % 3 = 0 THEN 'preferred' ELSE 'standard' END,
    seed.countries[((gs - 1) % array_length(seed.countries, 1)) + 1],
    7 + (gs % 18),
    DATE '2014-01-01' + ((gs * 9) % 3650)
FROM generate_series(1, 24) AS gs
CROSS JOIN seed;

INSERT INTO warehouses (
    warehouse_code,
    warehouse_name,
    region_name,
    timezone_name,
    capacity_units,
    opened_date
)
WITH seed AS (
    SELECT ARRAY[
        'Seattle', 'Los Angeles', 'Dallas', 'Chicago', 'Atlanta', 'New Jersey',
        'Toronto', 'London', 'Berlin', 'Singapore', 'Sydney', 'Mumbai'
    ]::text[] AS cities,
    ARRAY[
        'US-West', 'US-West', 'US-Central', 'US-Central', 'US-East', 'US-East',
        'Canada', 'Europe', 'Europe', 'APAC', 'APAC', 'India'
    ]::text[] AS regions,
    ARRAY[
        'America/Los_Angeles', 'America/Los_Angeles', 'America/Chicago', 'America/Chicago',
        'America/New_York', 'America/New_York', 'America/Toronto', 'Europe/London',
        'Europe/Berlin', 'Asia/Singapore', 'Australia/Sydney', 'Asia/Kolkata'
    ]::text[] AS timezones
)
SELECT
    format('WH-%s', lpad(gs::text, 2, '0')),
    seed.cities[gs] || ' Fulfillment Center',
    seed.regions[gs],
    seed.timezones[gs],
    75000 + (gs * 6500),
    DATE '2017-01-01' + (gs * 35)
FROM generate_series(1, 4) AS gs
CROSS JOIN seed;

INSERT INTO customers (
    customer_number,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    signup_date,
    acquisition_channel,
    loyalty_tier,
    is_active,
    marketing_opt_in
)
WITH seed AS (
    SELECT ARRAY['Ava', 'Noah', 'Mia', 'Liam', 'Sophia', 'Ethan', 'Isabella', 'Mason', 'Olivia', 'Lucas', 'Amelia', 'James', 'Harper', 'Elijah', 'Evelyn', 'Logan', 'Charlotte', 'Benjamin', 'Ella', 'Jackson']::text[] AS first_names,
           ARRAY['Patel', 'Kim', 'Garcia', 'Nguyen', 'Johnson', 'Smith', 'Brown', 'Chen', 'Martin', 'Lopez', 'Williams', 'Singh', 'Davis', 'Clark', 'Lewis', 'Walker', 'Allen', 'Young', 'Hall', 'Adams']::text[] AS last_names,
           ARRAY['organic_search', 'paid_search', 'social_media', 'email', 'affiliate', 'marketplace', 'referral']::text[] AS channels,
           ARRAY['bronze', 'silver', 'gold', 'platinum']::text[] AS tiers,
           ARRAY['female', 'male', 'non_binary']::text[] AS genders
)
SELECT
    'CUST-' || lpad(gs::text, 7, '0'),
    seed.first_names[((gs - 1) % array_length(seed.first_names, 1)) + 1],
    seed.last_names[(((gs - 1) / array_length(seed.first_names, 1)) % array_length(seed.last_names, 1)) + 1],
    lower(format('%s.%s.%05s@customer.dlcopilot.local',
        seed.first_names[((gs - 1) % array_length(seed.first_names, 1)) + 1],
        seed.last_names[(((gs - 1) / array_length(seed.first_names, 1)) % array_length(seed.last_names, 1)) + 1],
        gs
    )),
    format('+1-800-%03s-%04s', (gs * 7) % 1000, (gs * 97) % 10000),
    DATE '1970-01-01' + ((gs * 37) % 15000),
    seed.genders[((gs - 1) % array_length(seed.genders, 1)) + 1],
    DATE '2022-01-01' + ((gs * 5) % 1460),
    seed.channels[((gs - 1) % array_length(seed.channels, 1)) + 1],
    seed.tiers[((gs - 1) % array_length(seed.tiers, 1)) + 1],
    (gs % 29 <> 0),
    (gs % 5 <> 0)
FROM generate_series(1, 120) AS gs
CROSS JOIN seed;

INSERT INTO customer_addresses (
    customer_id,
    address_type,
    line1,
    city,
    state_code,
    postal_code,
    country_code,
    is_default,
    created_at
)
WITH seed AS (
    SELECT ARRAY['Seattle', 'Los Angeles', 'Dallas', 'Chicago', 'Atlanta', 'Newark', 'Toronto', 'Austin', 'Denver', 'Miami']::text[] AS cities,
           ARRAY['WA', 'CA', 'TX', 'IL', 'GA', 'NJ', 'ON', 'TX', 'CO', 'FL']::text[] AS states
)
SELECT
    c.customer_id,
    addr.address_type,
    format('%s %s %s', 100 + ((c.customer_id + addr.seq) % 9900),
        CASE WHEN addr.address_type = 'billing' THEN 'Market' ELSE 'Commerce' END,
        CASE WHEN (c.customer_id + addr.seq) % 3 = 0 THEN 'Street' WHEN (c.customer_id + addr.seq) % 3 = 1 THEN 'Avenue' ELSE 'Boulevard' END
    ),
    seed.cities[((c.customer_id + addr.seq - 1) % array_length(seed.cities, 1)) + 1],
    seed.states[((c.customer_id + addr.seq - 1) % array_length(seed.states, 1)) + 1],
    lpad(((c.customer_id * 17 + addr.seq * 13) % 99999)::text, 5, '0'),
    CASE WHEN c.customer_id % 9 = 0 THEN 'CA' ELSE 'US' END,
    TRUE,
    (c.signup_date + (addr.seq || ' hours')::interval)
FROM customers AS c
CROSS JOIN (
    VALUES (1, 'billing'), (2, 'shipping')
) AS addr(seq, address_type)
CROSS JOIN seed;

INSERT INTO products (
    sku,
    product_name,
    category_id,
    brand_id,
    color_name,
    size_label,
    unit_cost,
    unit_price,
    launch_date,
    average_rating,
    is_active
)
WITH seed AS (
    SELECT ARRAY['Smart', 'Ultra', 'Essential', 'Portable', 'Modern', 'Everyday', 'Premium', 'Compact', 'Performance', 'Elite']::text[] AS adjectives,
           ARRAY['Speaker', 'Desk', 'Sneaker', 'Mixer', 'Lamp', 'Backpack', 'Keyboard', 'Headset', 'Jacket', 'Bottle']::text[] AS nouns,
           ARRAY['Black', 'White', 'Navy', 'Sand', 'Silver', 'Olive', 'Rose', 'Graphite']::text[] AS colors,
           ARRAY['XS', 'S', 'M', 'L', 'XL', 'XXL', 'One Size', 'Standard']::text[] AS sizes
)
SELECT
    'SKU-' || lpad(gs::text, 8, '0'),
    format('%s %s %s',
        seed.adjectives[((gs - 1) % array_length(seed.adjectives, 1)) + 1],
        seed.nouns[(((gs - 1) / array_length(seed.adjectives, 1)) % array_length(seed.nouns, 1)) + 1],
        lpad(gs::text, 5, '0')
    ),
    ((gs - 1) % 12) + 1,
    ((gs - 1) % 18) + 1,
    seed.colors[((gs - 1) % array_length(seed.colors, 1)) + 1],
    seed.sizes[((gs - 1) % array_length(seed.sizes, 1)) + 1],
    round((5 + ((gs * 17) % 9000) / 100.0)::numeric, 2),
    round(((5 + ((gs * 17) % 9000) / 100.0) * (1.35 + ((gs % 25) / 100.0)))::numeric, 2),
    DATE '2021-01-01' + ((gs * 3) % 1825),
    round((3.20 + ((gs % 19) * 0.1))::numeric, 2),
    (gs % 41 <> 0)
FROM generate_series(1, 180) AS gs
CROSS JOIN seed;

INSERT INTO product_suppliers (
    product_id,
    supplier_id,
    supplier_sku,
    supply_cost,
    lead_time_days,
    is_primary,
    effective_from
)
SELECT
    p.product_id,
    ((p.product_id - 1) % 24) + 1,
    'SUP-' || p.product_id || '-A',
    round((p.unit_cost * 0.97)::numeric, 2),
    6 + (p.product_id % 12),
    TRUE,
    p.launch_date - ((p.product_id % 40) || ' days')::interval
FROM products AS p
UNION ALL
SELECT
    p.product_id,
    ((p.product_id + 7) % 24) + 1,
    'SUP-' || p.product_id || '-B',
    round((p.unit_cost * 1.02)::numeric, 2),
    10 + (p.product_id % 15),
    FALSE,
    p.launch_date - ((p.product_id % 22) || ' days')::interval
FROM products AS p;

INSERT INTO inventory_snapshots (
    snapshot_date,
    warehouse_id,
    product_id,
    on_hand_qty,
    reserved_qty,
    reorder_point_qty,
    incoming_qty
)
SELECT
    CURRENT_DATE - 1,
    w.warehouse_id,
    p.product_id,
    15 + ((p.product_id * w.warehouse_id) % 180),
    ((p.product_id + w.warehouse_id) % 18),
    12 + ((p.product_id + w.warehouse_id * 3) % 40),
    ((p.product_id * 5 + w.warehouse_id * 11) % 60)
FROM warehouses AS w
CROSS JOIN products AS p;

INSERT INTO orders (
    order_number,
    customer_id,
    billing_address_id,
    shipping_address_id,
    order_ts,
    order_status,
    sales_channel,
    currency_code,
    payment_status,
    fulfilled_ts
)
WITH seed AS (
    SELECT ARRAY['web', 'mobile_app', 'marketplace', 'store_assisted']::text[] AS channels
),
base_orders AS (
    SELECT
        gs,
        ((gs * 17) % 120) + 1 AS customer_id,
        TIMESTAMP '2024-01-01 00:00:00'
            + (((gs * 37) % 730) || ' days')::interval
            + (((gs * 13) % 86400) || ' seconds')::interval AS order_ts,
        CASE
            WHEN gs % 20 = 0 THEN 'CANCELLED'
            WHEN gs % 11 = 0 THEN 'RETURNED'
            WHEN gs % 5 = 0 THEN 'SHIPPED'
            WHEN gs % 3 = 0 THEN 'PROCESSING'
            ELSE 'DELIVERED'
        END AS order_status,
        seed.channels[((gs - 1) % array_length(seed.channels, 1)) + 1] AS sales_channel
    FROM generate_series(1, 420) AS gs
    CROSS JOIN seed
)
SELECT
    'ORD-' || lpad(base_orders.gs::text, 9, '0'),
    base_orders.customer_id,
    (base_orders.customer_id * 2) - 1,
    base_orders.customer_id * 2,
    base_orders.order_ts,
    base_orders.order_status,
    base_orders.sales_channel,
    'USD',
    CASE
        WHEN base_orders.order_status = 'CANCELLED' THEN 'VOIDED'
        WHEN base_orders.order_status = 'RETURNED' THEN 'REFUNDED'
        WHEN base_orders.order_status = 'PROCESSING' THEN 'AUTHORIZED'
        ELSE 'CAPTURED'
    END,
    CASE
        WHEN base_orders.order_status IN ('SHIPPED', 'DELIVERED', 'RETURNED')
            THEN base_orders.order_ts + (((base_orders.gs % 6) + 1) || ' days')::interval
        ELSE NULL
    END
FROM base_orders;

INSERT INTO order_items (
    order_id,
    product_id,
    warehouse_id,
    quantity,
    unit_price,
    discount_amount,
    gross_amount,
    net_amount,
    item_status
)
SELECT
    o.order_id,
    product_ref.product_id,
    ((o.order_id + item_ref.item_no * 3) % 4) + 1,
    qty.quantity,
    p.unit_price,
    round((qty.quantity * p.unit_price * CASE WHEN o.order_status = 'CANCELLED' THEN 0 ELSE ((o.order_id + item_ref.item_no) % 4) * 0.03 END)::numeric, 2),
    round((qty.quantity * p.unit_price)::numeric, 2),
    round((qty.quantity * p.unit_price * (1 - CASE WHEN o.order_status = 'CANCELLED' THEN 0 ELSE ((o.order_id + item_ref.item_no) % 4) * 0.03 END))::numeric, 2),
    CASE
        WHEN o.order_status = 'CANCELLED' THEN 'CANCELLED'
        WHEN o.order_status = 'RETURNED' AND item_ref.item_no = 1 THEN 'RETURNED'
        WHEN o.order_status IN ('SHIPPED', 'DELIVERED', 'RETURNED') THEN 'FULFILLED'
        ELSE 'PENDING'
    END
FROM orders AS o
CROSS JOIN (VALUES (1), (2), (3)) AS item_ref(item_no)
CROSS JOIN LATERAL (
    SELECT ((o.order_id * 13 + item_ref.item_no * 997) % 180) + 1 AS product_id
) AS product_ref
CROSS JOIN LATERAL (
    SELECT 1 + ((o.order_id + item_ref.item_no) % 4) AS quantity
) AS qty
JOIN products AS p
    ON p.product_id = product_ref.product_id;

WITH order_rollup AS (
    SELECT
        oi.order_id,
        round(sum(oi.gross_amount)::numeric, 2) AS subtotal_amount,
        round(sum(oi.discount_amount)::numeric, 2) AS discount_amount,
        round(sum(oi.net_amount)::numeric, 2) AS merchandise_amount
    FROM order_items AS oi
    GROUP BY oi.order_id
)
UPDATE orders AS o
SET subtotal_amount = order_rollup.subtotal_amount,
    discount_amount = order_rollup.discount_amount,
    shipping_amount = CASE WHEN order_rollup.merchandise_amount >= 100 THEN 0 ELSE 7.99 + (o.order_id % 3) END,
    tax_amount = round((order_rollup.merchandise_amount * 0.08)::numeric, 2),
    total_amount = round((
        order_rollup.merchandise_amount
        + CASE WHEN order_rollup.merchandise_amount >= 100 THEN 0 ELSE 7.99 + (o.order_id % 3) END
        + round((order_rollup.merchandise_amount * 0.08)::numeric, 2)
    )::numeric, 2)
FROM order_rollup
WHERE o.order_id = order_rollup.order_id;

INSERT INTO payments (
    order_id,
    payment_ts,
    payment_method,
    amount,
    captured_amount,
    payment_status,
    gateway_name,
    authorization_code
)
SELECT
    o.order_id,
    o.order_ts + ((o.order_id % 25) || ' minutes')::interval,
    CASE (o.order_id % 5)
        WHEN 0 THEN 'card'
        WHEN 1 THEN 'paypal'
        WHEN 2 THEN 'apple_pay'
        WHEN 3 THEN 'gift_card'
        ELSE 'buy_now_pay_later'
    END,
    o.total_amount,
    CASE
        WHEN o.payment_status IN ('CAPTURED', 'REFUNDED') THEN o.total_amount
        ELSE 0
    END,
    o.payment_status,
    CASE (o.order_id % 4)
        WHEN 0 THEN 'stripe'
        WHEN 1 THEN 'adyen'
        WHEN 2 THEN 'paypal'
        ELSE 'braintree'
    END,
    upper(substr(md5(o.order_number || '-' || o.customer_id), 1, 12))
FROM orders AS o;

INSERT INTO shipments (
    order_id,
    warehouse_id,
    carrier_name,
    tracking_number,
    shipped_at,
    delivered_at,
    shipping_service,
    shipment_status,
    freight_cost
)
SELECT
    o.order_id,
    ((o.order_id % 4) + 1),
    CASE (o.order_id % 4)
        WHEN 0 THEN 'UPS'
        WHEN 1 THEN 'FedEx'
        WHEN 2 THEN 'USPS'
        ELSE 'DHL'
    END,
    'TRK-' || lpad(o.order_id::text, 10, '0'),
    CASE
        WHEN o.order_status IN ('SHIPPED', 'DELIVERED', 'RETURNED') THEN o.order_ts + (((o.order_id % 3) + 1) || ' days')::interval
        ELSE NULL
    END,
    CASE
        WHEN o.order_status IN ('DELIVERED', 'RETURNED') THEN o.order_ts + (((o.order_id % 6) + 3) || ' days')::interval
        ELSE NULL
    END,
    CASE (o.order_id % 3)
        WHEN 0 THEN 'standard'
        WHEN 1 THEN 'expedited'
        ELSE 'economy'
    END,
    CASE
        WHEN o.order_status = 'CANCELLED' THEN 'CANCELLED'
        WHEN o.order_status = 'PROCESSING' THEN 'PENDING'
        ELSE o.order_status
    END,
    round((5 + ((o.order_id % 11) * 1.15))::numeric, 2)
FROM orders AS o;

INSERT INTO reviews (
    product_id,
    customer_id,
    order_id,
    rating,
    review_title,
    review_body,
    review_date,
    is_verified_purchase,
    helpful_votes
)
WITH ranked_reviews AS (
    SELECT
        oi.product_id,
        o.customer_id,
        o.order_id,
        o.fulfilled_ts,
        row_number() OVER (ORDER BY o.order_id, oi.order_item_id) AS review_rank
    FROM orders AS o
    JOIN order_items AS oi ON oi.order_id = o.order_id
    WHERE o.order_status IN ('DELIVERED', 'RETURNED')
)
SELECT
    ranked_reviews.product_id,
    ranked_reviews.customer_id,
    ranked_reviews.order_id,
    3 + (ranked_reviews.review_rank % 3),
    CASE ranked_reviews.review_rank % 4
        WHEN 0 THEN 'Exactly what I needed'
        WHEN 1 THEN 'Reliable quality'
        WHEN 2 THEN 'Great value for money'
        ELSE 'Would recommend'
    END,
    CASE ranked_reviews.review_rank % 4
        WHEN 0 THEN 'Delivery was on time and the product matched the listing.'
        WHEN 1 THEN 'Packaging was good and setup was straightforward.'
        WHEN 2 THEN 'The item performs well for everyday use and feels durable.'
        ELSE 'Overall a strong purchase with only minor trade-offs.'
    END,
    (ranked_reviews.fulfilled_ts::date + ((ranked_reviews.review_rank % 21) || ' days')::interval)::date,
    TRUE,
    ranked_reviews.review_rank % 27
FROM ranked_reviews
WHERE ranked_reviews.review_rank <= 260;

INSERT INTO returns (
    order_item_id,
    return_ts,
    return_reason,
    return_status,
    refund_amount,
    restockable
)
WITH ranked_returns AS (
    SELECT
        oi.order_item_id,
        o.fulfilled_ts,
        oi.net_amount,
        row_number() OVER (ORDER BY oi.order_item_id) AS return_rank
    FROM orders AS o
    JOIN order_items AS oi ON oi.order_id = o.order_id
    WHERE oi.item_status = 'RETURNED'
)
SELECT
    ranked_returns.order_item_id,
    ranked_returns.fulfilled_ts + ((ranked_returns.return_rank % 10) || ' days')::interval,
    CASE ranked_returns.return_rank % 5
        WHEN 0 THEN 'Damaged in transit'
        WHEN 1 THEN 'Wrong size'
        WHEN 2 THEN 'Changed mind'
        WHEN 3 THEN 'Not as described'
        ELSE 'Late delivery'
    END,
    CASE WHEN ranked_returns.return_rank % 6 = 0 THEN 'INSPECTING' ELSE 'REFUNDED' END,
    ranked_returns.net_amount,
    (ranked_returns.return_rank % 7 <> 0)
FROM ranked_returns
WHERE ranked_returns.return_rank <= 70;

CREATE VIEW vw_customer_lifetime_value AS
SELECT
    c.customer_id,
    c.customer_number,
    c.first_name,
    c.last_name,
    c.loyalty_tier,
    count(o.order_id) FILTER (WHERE o.order_status <> 'CANCELLED') AS completed_order_count,
    round(coalesce(sum(o.total_amount) FILTER (WHERE o.order_status <> 'CANCELLED'), 0)::numeric, 2) AS gross_revenue,
    round(coalesce(avg(o.total_amount) FILTER (WHERE o.order_status <> 'CANCELLED'), 0)::numeric, 2) AS average_order_value,
    max(o.order_ts) AS last_order_ts
FROM customers AS c
LEFT JOIN orders AS o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_number, c.first_name, c.last_name, c.loyalty_tier;

CREATE VIEW vw_daily_sales_summary AS
SELECT
    o.order_ts::date AS order_date,
    o.sales_channel,
    count(*) FILTER (WHERE o.order_status <> 'CANCELLED') AS order_count,
    round(sum(o.total_amount) FILTER (WHERE o.order_status <> 'CANCELLED')::numeric, 2) AS gross_sales,
    round(sum(o.discount_amount)::numeric, 2) AS total_discount,
    round(sum(o.tax_amount)::numeric, 2) AS total_tax,
    round(avg(o.total_amount) FILTER (WHERE o.order_status <> 'CANCELLED')::numeric, 2) AS avg_order_value
FROM orders AS o
GROUP BY o.order_ts::date, o.sales_channel;

CREATE VIEW vw_inventory_alerts AS
SELECT
    i.snapshot_date,
    w.warehouse_code,
    p.sku,
    p.product_name,
    c.category_name,
    i.on_hand_qty,
    i.reserved_qty,
    i.reorder_point_qty,
    i.incoming_qty,
    CASE
        WHEN i.on_hand_qty - i.reserved_qty <= i.reorder_point_qty THEN 'REORDER_NOW'
        WHEN i.on_hand_qty - i.reserved_qty <= i.reorder_point_qty + 10 THEN 'WATCHLIST'
        ELSE 'HEALTHY'
    END AS inventory_health
FROM inventory_snapshots AS i
JOIN warehouses AS w ON w.warehouse_id = i.warehouse_id
JOIN products AS p ON p.product_id = i.product_id
JOIN categories AS c ON c.category_id = p.category_id;

ANALYZE;