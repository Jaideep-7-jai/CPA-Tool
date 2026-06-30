-- CPA Tool Schema
-- Run once to initialise / migrate

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Drop old requests table if migrating from old schema (backup data first!)
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS ... (use below CREATE for fresh installs)

CREATE TABLE IF NOT EXISTS requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    request_uuid    VARCHAR(64)  NOT NULL UNIQUE,
    request_name    VARCHAR(255) NOT NULL UNIQUE COMMENT 'Human-readable unique name',
    request_type    ENUM('Suppression','Mailing','Doordash') NOT NULL DEFAULT 'Suppression',
    client_name     VARCHAR(255) NOT NULL DEFAULT '',
    created_by      INT NOT NULL,
    criteria_type   ENUM('age','state','zips') NOT NULL,
    comp_type       ENUM('greater','less','include','exclude') NOT NULL DEFAULT 'include',
    channel         ENUM('ALL','GREEN','BLUE','ORANGE','ARCAMAX') NOT NULL DEFAULT 'ALL',
    criteria_value  VARCHAR(500) NULL COMMENT 'age value or state list; NULL for zips/doordash',
    zip_file_path   VARCHAR(500) NULL,
    output_dir      VARCHAR(255) NOT NULL,
    overall_status  ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress',
    command_text    TEXT NULL,
    log_file        VARCHAR(500) NULL,
    stdout_text     MEDIUMTEXT NULL,
    stderr_text     MEDIUMTEXT NULL,
    return_code     INT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at      DATETIME NULL,
    finished_at     DATETIME NULL,
    FOREIGN KEY (created_by) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Migration helper: add new columns to existing installs
-- Run these only if upgrading from old schema:
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS request_name VARCHAR(255) UNIQUE AFTER request_uuid;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS request_type ENUM('Suppression','Mailing','Doordash') NOT NULL DEFAULT 'Suppression' AFTER request_name;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS client_name VARCHAR(255) NOT NULL DEFAULT '' AFTER request_type;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS criteria_type ENUM('age','state','zips') NOT NULL AFTER client_name;
-- ALTER TABLE requests MODIFY criteria ENUM('age','state','zips') NOT NULL;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS comp_type ENUM('greater','less','include','exclude') NOT NULL DEFAULT 'include' AFTER criteria_type;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS channel ENUM('ALL','GREEN','BLUE','ORANGE','ARCAMAX') NOT NULL DEFAULT 'ALL' AFTER comp_type;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS criteria_value VARCHAR(500) NULL AFTER channel;
-- ALTER TABLE requests ADD COLUMN IF NOT EXISTS overall_status ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress';
