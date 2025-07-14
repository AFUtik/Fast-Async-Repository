create table if not exists users (
    id BIGINT PRIMARY KEY,
    tag VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);