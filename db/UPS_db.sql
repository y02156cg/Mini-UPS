-- CREATE TABLE users (
--     id SERIAL PRIMARY KEY,
--     username VARCHAR(50) UNIQUE NOT NULL,
--     password_hash VARCHAR(255) NOT NULL,
--     email VARCHAR(100) UNIQUE,
--     created_time TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE trucks (
--     id INTEGER PRIMARY KEY,
--     status VARCHAR(20) NOT NULL DEFAULT 'idle',
--     x INTEGER NOT NULL DEFAULT 0,
--     y INTEGER NOT NULL DEFAULT 0,
--     world_id BIGINT NOT NULL,
--     updated_at TIMESTAMP NOT NULL DEFAULT NOW()  -- 
-- );

-- CREATE TABLE warehouses (
--     id INTEGER PRIMARY KEY,
--     x INTEGER NOT NULL,
--     y INTEGER NOT NULL,
--     world_id BIGINT NOT NULL
-- );

-- CREATE TABLE packages (
--     id VARCHAR(50) PRIMARY KEY,
--     user_id INTEGER REFERENCES auth_user(id), 
--     truck_id INTEGER REFERENCES trucks(id),
--     warehouse_id INTEGER REFERENCES warehouses(id),
--     status VARCHAR(20) NOT NULL DEFAULT 'created',
--     destination_x INTEGER,
--     destination_y INTEGER,
--     description TEXT,
--     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE items (
--     id SERIAL PRIMARY KEY,
--     package_id VARCHAR(50) REFERENCES packages(id) ON DELETE CASCADE,
--     name VARCHAR(100) NOT NULL,
--     description TEXT,
--     quantity INTEGER NOT NULL DEFAULT 1
-- );

-- CREATE TABLE notifications (
--     id SERIAL PRIMARY KEY,
--     user_id INTEGER REFERENCES auth_user(id) ON DELETE CASCADE,
--     message TEXT NOT NULL,
--     read BOOLEAN NOT NULL DEFAULT FALSE,
--     created_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE error_logs (
--     id SERIAL PRIMARY KEY,
--     seqnum BIGINT,
--     error_message TEXT NOT NULL,
--     created_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE wrold_state(
--     id SERIAL PRIMARY KEY,
--     world_id BIGINT NOT NULL UNIQUE,
--     sim_speed INTEGER NOT NULL DEFAULT 100,
--     connected_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE sequence_num(
--     id SERIAL PRIMARY KEY,
--     last_seq_num BIGINT NOT NULL DEFAULT 0,
--     last_ack_received BIGINT NOT NULL DEFAULT 0,
--     updated_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- CREATE TABLE amazon_messages(
--     id SERIAL PRIMARY KEY,
--     message_type VARCHAR(50) NOT NULL,
--     message_content JSONB NOT NULL,
--     status VARCHAR(20) NOT NULL DEFAULT 'pending',
--     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
--     processed_at TIMESTAMP
-- );

-- CREATE INDEX packages_user_id_idx ON packages(user_id);
-- CREATE INDEX packages_truck_id_idx ON packages(truck_id);
-- CREATE INDEX packages_warehouse_id_idx ON packages(warehouse_id);
-- CREATE INDEX packages_status_id_idx ON packages(status);
-- CREATE INDEX items_package_id_idx ON items(package_id);
-- CREATE INDEX notifications_user_id_idx ON notifications(user_id);
-- CREATE INDEX notifications_read_id_idx ON notifications(read);
-- CREATE INDEX amazon_message_status_idx ON amazon_message(status);

-- initialize 10 default trucks 
-- INSERT INTO trucks (id, status, world_id)
-- SELECT t, 'idle', 0
-- FROM generate_series(1, 10) AS t;
-- DROP TABLE IF EXISTS users;