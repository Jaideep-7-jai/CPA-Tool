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
    -- VARCHAR instead of ENUM so multi-channel strings like 'GREEN,ORANGE' are stored correctly
    channel         VARCHAR(100) NOT NULL DEFAULT 'ALL',
    criteria_value  VARCHAR(500) NULL COMMENT 'age value or state list; NULL for zips/doordash',
    zip_file_path   VARCHAR(500) NULL,
    output_dir      VARCHAR(255) NOT NULL,
    overall_status  ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress',
    GREEN_STATUS    VARCHAR(50)  NULL,
    BLUE_STATUS     VARCHAR(50)  NULL,
    ARCAMAX_STATUS  VARCHAR(50)  NULL,
    ORANGE_STATUS   VARCHAR(50)  NULL,
    APPTNESS_STATUS VARCHAR(50)  NULL,
    GREEN_FTP       VARCHAR(500) NULL,
    BLUE_FTP        VARCHAR(500) NULL,
    ARCAMAX_FTP     VARCHAR(500) NULL,
    ORANGE_FTP      VARCHAR(500) NULL,
    GREEN_FILECOUNT VARCHAR(50)  NULL,
    BLUE_FILECOUNT  VARCHAR(50)  NULL,
    ARCAMAX_FILECOUNT VARCHAR(50) NULL,
    ORANGE_FILECOUNT VARCHAR(50) NULL,
    GREEN_FILENAME  VARCHAR(500) NULL,
    BLUE_FILENAME   VARCHAR(500) NULL,
    ARCAMAX_FILENAME VARCHAR(500) NULL,
    ORANGE_FILENAME VARCHAR(500) NULL,
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

-- ─── Migration helpers (run on existing installs) ───────────────────────────
-- Fix channel column: ENUM -> VARCHAR so multi-channel values like 'GREEN,ORANGE' work
ALTER TABLE requests MODIFY COLUMN channel VARCHAR(100) NOT NULL DEFAULT 'ALL';

-- Add per-channel status / FTP / filecount / filename columns if missing
ALTER TABLE requests ADD COLUMN IF NOT EXISTS APPTNESS_STATUS  VARCHAR(50)  NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS GREEN_FTP        VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS BLUE_FTP         VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ARCAMAX_FTP      VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ORANGE_FTP       VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS GREEN_FILECOUNT  VARCHAR(50)  NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS BLUE_FILECOUNT   VARCHAR(50)  NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ARCAMAX_FILECOUNT VARCHAR(50) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ORANGE_FILECOUNT VARCHAR(50)  NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS GREEN_FILENAME   VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS BLUE_FILENAME    VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ARCAMAX_FILENAME VARCHAR(500) NULL;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS ORANGE_FILENAME  VARCHAR(500) NULL;

-- filedetails table
CREATE TABLE IF NOT EXISTS filedetails (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    requestid    VARCHAR(64)  NOT NULL,
    requestname  VARCHAR(255) NOT NULL,
    filespath    VARCHAR(500) NULL,
    jsondata     MEDIUMTEXT   NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_requestid (requestid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
