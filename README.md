# CPA Request Portal

This is a Flask + MySQL frontend for your existing suppression CLI script. Your current script already supports `--criteria age|state|zip`, `--comp`, `--age`, `--states`, `--zip-file`, and `--output-dir`, so the web app mirrors those same rules instead of replacing the underlying processing flow.[file:1]

## Modules

### `app.py`
Main Flask application. It handles login, session protection, MySQL reads/writes, request submission, file upload handling, background execution, and request status updates.[web:29][web:32]

### `templates/login.html`
Login page shown before the user can access the app. It posts username and password to Flask and displays login errors if credentials fail.[web:29]

### `templates/home.html`
Authenticated home page. The main panel contains only the create-request form, and the sidebar shows all created requests, matching your requested layout.[web:32]

### `static/app.js`
Frontend controller for the home page. It changes comparison values based on criteria, shows and hides the correct inputs, submits the form using `fetch`, resets the form after success, and refreshes the sidebar list periodically.[web:31]

### `static/style.css`
App styling for login page, home page, sidebar, form, status badges, and responsive layout.

### `schema.sql`
MySQL schema for the `users` and `requests` tables.

### `requirements.txt`
Python dependencies needed to run the app.

## Flow

1. User opens `/login` and signs in using credentials stored in MySQL.[web:29]
2. Flask checks the password hash with Werkzeug instead of comparing plain text passwords.[web:32]
3. After login, Flask redirects to `/` and shows the create-request home page.[web:29]
4. The user fills the request form. Validation follows the exact CLI rules from your script for age, state, and zip inputs.[file:1]
5. When submitted, a new row is inserted into MySQL `requests` with initial status `inprogress`.[web:31]
6. A background thread launches the real suppression script with the equivalent CLI command.[web:31]
7. When execution ends, the request row is updated to `completed` or `failed` using the subprocess return code, and stdout/stderr/log file details are stored.[web:31][web:37]
8. The sidebar polls `/api/requests` every 5 seconds so users can see current status changes without refreshing the page.[web:31]
9. After a successful request, the form resets automatically so the create page is clean for the next submission.

## Setup

### Install packages

```bash
pip3.6 install -r requirements.txt
```

### Create database

```bash
mysql -u root -p < schema.sql
```

### Set environment variables

```bash
export APP_DB_HOST=localhost
export APP_DB_USER=root
export APP_DB_PASSWORD='your_password'
export APP_DB_NAME=cpa_request_portal
export FLASK_SECRET_KEY='change-me'
export SUPPRESSION_SCRIPT_PATH='/full/path/to/main.py'
export APP_DEFAULT_ADMIN='admin'
export APP_DEFAULT_ADMIN_PASSWORD='admin123'
```

### Run

```bash
python3.6 app.py
```

The app is configured with `host='0.0.0.0'`, so access it using the server hostname or IP rather than `127.0.0.1` from your own desktop browser.[cite:1]

## Notes

- Replace `SUPPRESSION_SCRIPT_PATH` with your real existing script path if the CLI project is outside this web app folder.[file:1]
- You can later add registration, per-user request filtering, request detail modal, and log viewing endpoints without changing the basic flow.[web:29][web:31]
