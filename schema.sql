CREATE DATABASE IF NOT EXISTS cpa_request_portal;
USE cpa_request_portal;

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    request_uuid VARCHAR(64) NOT NULL UNIQUE,
    created_by INT NOT NULL,
    criteria ENUM('age','state','zip') NOT NULL,
    comp ENUM('greater','less','include','exclude') NOT NULL,
    age INT NULL,
    states VARCHAR(500) NULL,
    zip_file_path VARCHAR(500) NULL,
    output_dir VARCHAR(255) NOT NULL,
    status ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress',
    command_text TEXT NULL,
    log_file VARCHAR(500) NULL,
    stdout_text MEDIUMTEXT NULL,
    stderr_text MEDIUMTEXT NULL,
    return_code INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    FOREIGN KEY (created_by) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
