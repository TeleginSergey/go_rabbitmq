CREATE TABLE IF NOT EXISTS messages_consumer1 (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
);
CREATE TABLE IF NOT EXISTS messages_consumer2 (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
);