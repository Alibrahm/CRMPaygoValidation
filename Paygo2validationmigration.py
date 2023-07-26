import psycopg2
import datetime
paygo_db_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/paygo"
# Target database connection string
target_db_connection_string = "postgresql://postgres:NlQA1fKgUGZGhPYqiCFx@auth-manager.cpnlo4orrspa.eu-west-1.rds.amazonaws.com:5432/paygovalidation"

def fetch_data_from_paygo_production_device():
    try:
        # Connect to the source database
        source_conn = psycopg2.connect(paygo_db_connection_string)
        source_cursor = source_conn.cursor()

        # Execute the query to fetch data from the source database
        query = """
            SELECT
                id,
                created_at,
                daily_rate,
                expiry_date,
                serial_no 
            FROM device
            ORDER BY id ASC 
           ;
                """   
        source_cursor.execute(query)
        # Fetch all the rows from the query result
        rows = source_cursor.fetchall()
        # Close the source database connection
        source_cursor.close()
        source_conn.close()

        return rows

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the source database:", error)



def fetch_data_from_paygo_production_wallet():
    try:
        # Connect to the source database
        source_conn = psycopg2.connect(paygo_db_connection_string)
        source_cursor = source_conn.cursor()

        # Execute the query to fetch data from the source database
        query = """
            SELECT
                id,
                account_no,
                balance,
                created_at,
                device_id
            FROM wallet
            ORDER BY id ASC 
           ;
                """   
        source_cursor.execute(query)
        # Fetch all the rows from the query result
        rows = source_cursor.fetchall()
        # Close the source database connection
        source_cursor.close()
        source_conn.close()

        return rows

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the source database table wallet:", error)


def migrate_data_to_target_paygoproduction_table(data):
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # Prepare the SQL query for inserting data into the target table
        query = """
            INSERT INTO paygoproduction (id, created_at, daily_rate, expiry_date, serial_no)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET 
                created_at = EXCLUDED.created_at,
                daily_rate = EXCLUDED.daily_rate,
                expiry_date = EXCLUDED.expiry_date,
                serial_no = EXCLUDED.serial_no;
        """

        # Execute the query for each row of data
        for row in data:
            target_cursor.execute(query, row)

        # Commit the changes to the target database
        target_conn.commit()

        # Close the target database connection
        target_cursor.close()
        target_conn.close()

        print("Data migration to the target database paygo validation table successful.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database or inserting data:", error)

def migrate_data_to_target_paygoproduction_wallet(data):
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # Prepare the SQL query for inserting data into the target table
        query = """
            INSERT INTO walletvalidation (id, account_no, balance, created_at, device_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET 
                account_no = EXCLUDED.account_no,
                balance = EXCLUDED.balance,
                created_at = EXCLUDED.created_at,
                device_id = EXCLUDED.device_id;
        """

        # Execute the query for each row of data
        for row in data:
            target_cursor.execute(query, row)

        # Commit the changes to the target database
        target_conn.commit()

        # Close the target database connection
        target_cursor.close()
        target_conn.close()

        print("Data migration to the target database table wallet validation successful.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database or inserting data in wallet validation:", error)


# Fetch data from the source database
data = fetch_data_from_paygo_production_device()
datawallet = fetch_data_from_paygo_production_wallet()

# Insert data into the target database
migrate_data_to_target_paygoproduction_table(data)
if datawallet is not None:
    migrate_data_to_target_paygoproduction_wallet(datawallet)
    
else:
    print("No data fetched from the source wallet paygo database.")