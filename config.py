
#!/usr/bin/env python3
SNOWSQL_PASSPHRASE = ''
AWS_KEY_ID = ''
AWS_SECRET_KEY = ''

# Email config
SENDER = 'datateam@zds-prod-job-03.bo3.e-dialog.com'
RECIPIENT = "jmolugu@zetaglobal.com"
CC_RECIPIENTS = "jmolugu@zetaglobal.com"

DB_HOST = 'zds-prod-pgdb02-01.bo3.e-dialog.com'
S3_BASE = 's3://temporary-data/DATATEAM_DND_FILES/CPA_SUPP_MALNG_REQ'


DB_CONFIG = {
    'orange': {'host': 'zds-prod-pgdb02-01.bo3.e-dialog.com', 'db': 'orange_db', 'user': 'datateam'},
    'arcamax': {'host': 'zds-prod-pgdb02-01.bo3.e-dialog.com', 'db': 'arcamax_db', 'user': 'datateam'},
    'mt2': {'host': 'cmprep-prod-ro-02-vip.bo3.e-dialog.com', 'db': 'mt2_data', 'user': 'cmp_cust_user', 'password': 'Cmp12cst01!!'}
}

