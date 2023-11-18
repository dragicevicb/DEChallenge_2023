CREATE EXTENSION IF NOT EXISTS plpgsql;

CREATE TABLE users (
    user_id UUID  PRIMARY KEY,
    country VARCHAR(255),
    registration_timestamp TIMESTAMP,
    device_os VARCHAR(255),
    marketing_campaign VARCHAR(255)
);

CREATE TABLE sessions (
    session_id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(user_id),
    login_timestamp TIMESTAMP,
    logout_timestamp TIMESTAMP,
    session_length_seconds INT,
    session_valid BOOLEAN DEFAULT FALSE,
    session_ended BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE FUNCTION update_session_bools()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.session_length_seconds IS NULL OR NEW.session_length_seconds < 1 THEN
        NEW.session_valid := FALSE;
    ELSE
        NEW.session_valid := TRUE;
    END IF;

    IF NEW.logout_timestamp IS NOT NULL THEN
        NEW.session_ended := TRUE;
    ELSE
        NEW.session_ended := FALSE;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER session_length_trigger
BEFORE INSERT OR UPDATE ON sessions
FOR EACH ROW EXECUTE FUNCTION update_session_bools();

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(user_id),
    transaction_timestamp TIMESTAMP,
    amount DECIMAL(10, 2),
    currency VARCHAR(3)
);
