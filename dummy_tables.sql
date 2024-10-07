CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    description VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO events (description, updated_at) VALUES ('Mi primer evento', '2023-01-01 10:00:00');

INSERT INTO events (description, updated_at) VALUES ('Evento antiguo', '2023-01-01 10:00:00');

INSERT INTO events (description, updated_at) VALUES ('Otro evento', NOW());

SELECT * FROM events


CREATE TABLE dimension (
    id SERIAL PRIMARY KEY,
    description VARCHAR(255)
);

INSERT INTO dimension (description) VALUES ('tt');
