from math import ceil, floor
import requests
import psycopg2
from datetime import date, datetime, timedelta
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders



# paygo_db_connection_string = "postgresql://postgres:NlQA1fKgUGZGhPYqiCFx@auth-manager.cpnlo4orrspa.eu-west-1.rds.amazonaws.com:5432/paygo"
paygo_db_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/paygo"
paymentsdb_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/paymentms"
validation_dbconnection_string =  "postgresql://postgres:NlQA1fKgUGZGhPYqiCFx@auth-manager.cpnlo4orrspa.eu-west-1.rds.amazonaws.com:5432/paygovalidation"

connection_payments = psycopg2.connect(paymentsdb_connection_string)
connection_paygo = psycopg2.connect(paygo_db_connection_string)
connection_validation =psycopg2.connect(validation_dbconnection_string)

cursor_payments = connection_payments.cursor()
cursor_paygo = connection_paygo.cursor()
cursor_validation = connection_validation.cursor()

table_name = 'transactions'
previous_day = datetime.utcnow().date() - timedelta(days=1)
query = f"SELECT amount, bill_reference_number, created_on, paid_name FROM {table_name} WHERE DATE(created_on) = '{previous_day}' "


cursor_payments.execute(query)
rows = cursor_payments.fetchall()



def process_bill_reference_numbers(rows):
    url = "https://hyzxqf8bwr.eu-west-1.awsapprunner.com/fineract-provider/api/v1/loans?externalId={}"
    headers = {
        'fineract-platform-tenantid': 'default',
        'Authorization': 'Basic bWlmb3M6OUtxeSZzcDgmRCFp'
    }

    # Create a dictionary to group rows by bill_reference_number and aggregate amounts
    bill_reference_groups = {}

    for row in rows:
        amount = row[0]
        bill_reference_number = row[1]
        created_on = row[2]
        paid_name = row[3]

        # Add or update the accumulated amount for the current bill_reference_number
        if bill_reference_number in bill_reference_groups:
            bill_reference_groups[bill_reference_number]['amount'] += amount
        else:
            bill_reference_groups[bill_reference_number] = {
                'amount': amount,
                'created_on': created_on,
                'paid_name': paid_name
            }

        full_url = url.format(bill_reference_number)
        response = requests.get(full_url, headers=headers)
        data = response.json()

        if 'pageItems' in data:
            page_items = data['pageItems']
            for item in page_items:
                if 'loanProductId' in item:
                    loan_product_id = item['loanProductId']
                    validation_daily_rate = fetch_loan_product_record(loan_product_id)

                    balance, device_id = fetch_balance_from_paygo_db(bill_reference_number, cursor_validation)
                    if balance is not None:
                        daily_rate, expiry_date, serial_no = fetch_device_details(device_id, cursor_validation)
                        num_days = (bill_reference_groups[bill_reference_number]['amount'] + float(balance)) / validation_daily_rate
                        new_wallet_balance = round((bill_reference_groups[bill_reference_number]['amount'] + float(balance)) % floor(validation_daily_rate))
                        expiry_date_str = str(expiry_date)
                        expiry_date_datetime = datetime.strptime(expiry_date_str, '%Y-%m-%d %H:%M:%S')
                       
                        # Check if the created_on date is ahead of the expiry_date_str
                        if bill_reference_groups[bill_reference_number]['created_on'] > expiry_date_datetime:
                            # If true, set expiry_due_dates as group['created_on'] plus timedelta(days=num_days)
                            expiry_date_str = str(bill_reference_groups[bill_reference_number]['created_on']) 
                            expiry_due_dates = datetime.strptime(expiry_date_str, '%Y-%m-%d  %H:%M:%S').date() + timedelta(days=num_days)
                        else:
                            # If false, set expiry_due_dates as expiry_date_datetime plus timedelta(days=num_days)
                            expiry_date_str = str(expiry_date)
                            expiry_due_dates = datetime.strptime(expiry_date_str, '%Y-%m-%d  %H:%M:%S').date() + timedelta(days=num_days)
                
                        update_device_details_in_paygo_db(device_id, validation_daily_rate, expiry_due_dates, cursor_validation)
                    else:
                        # daily_rate, expiry_date, serial_no = fetch_device_details(device_id, cursor_validation)
                        num_days = bill_reference_groups[bill_reference_number]['amount'] / validation_daily_rate
                        new_wallet_balance = round(bill_reference_groups[bill_reference_number]['amount'] % floor(validation_daily_rate))

                    num_days = ceil(num_days)

                    # Update the created_on value to the latest one
                    bill_reference_groups[bill_reference_number]['created_on'] = created_on

                    # Add the additional data to the bill_reference_group
                    bill_reference_groups[bill_reference_number]['num_days'] = num_days
                    bill_reference_groups[bill_reference_number]['validation_daily_rate'] = round(validation_daily_rate, 2)
                    bill_reference_groups[bill_reference_number]['balance'] = balance
                    bill_reference_groups[bill_reference_number]['new_wallet_balance'] = new_wallet_balance
                    bill_reference_groups[bill_reference_number]['daily_rate'] = daily_rate
                    bill_reference_groups[bill_reference_number]['expiry_date'] = expiry_date
                    bill_reference_groups[bill_reference_number]['serial_no'] = serial_no
                    bill_reference_groups[bill_reference_number]['expiry_due_dates'] = expiry_due_dates
                    

    # Create the result list with combined rows
    result = [(group['paid_name'], bill_ref, group['amount'], group['created_on'], group['num_days'],
               group['validation_daily_rate'], group['balance'], group['new_wallet_balance'],
               group['daily_rate'], group['expiry_date'], group['serial_no'], group['expiry_due_dates'])
              for bill_ref, group in bill_reference_groups.items()]
    
    # return result
    # Create a DataFrame from the result list
    report = pd.DataFrame(result, columns=['Customer Name', 'Account Number', 'Latest Paid Amount', 'Latest Transaction Date', 'Expiration due (days)', 'Validation Daily Rate', 'Paygo Wallet', 'New wallet Balance', 'Paygo Daily Rate','Paygo Due Date',  'Device Serial', 'Validation Due Date', ])
  
    for bill_reference_number, group in bill_reference_groups.items():
        # Update the balance in the wallet validation database
        update_balance_in_paygo_db(bill_reference_number, group['new_wallet_balance'], cursor_validation)
        
    # Save the DataFrame as an Excel file
    output_file = 'Validation.xlsx'
    report.to_excel(output_file, index=True)
    print(report)

    # Return the file path of the Excel file
    return report
                    
    
def fetch_loan_product_record(loan_product_id):
    url = f"https://hyzxqf8bwr.eu-west-1.awsapprunner.com/fineract-provider/api/v1/loanproducts/{loan_product_id}?associations=all&exclude=guarantors,futureSchedule/"
    headers = {
        'fineract-platform-tenantid': 'default',
        'Authorization': 'Basic bWlmb3M6OUtxeSZzcDgmRCFp'
    }
    
    response = requests.get(url, headers=headers)
    data = response.json()

    repayment_frequency_type = data.get('repaymentFrequencyType')
    frequency_type = data['repaymentFrequencyType']['value']
    
    if frequency_type == 'Days':
        validation_daily_rate = data['installmentAmountInMultiplesOf']
    elif frequency_type == 'Weeks':
        validation_daily_rate = data['installmentAmountInMultiplesOf'] / 7
    elif frequency_type == 'Months':
        validation_daily_rate = data['installmentAmountInMultiplesOf'] / 30
    
    return validation_daily_rate

# def fetch_balance_from_paygo_db(bill_reference_number, cursor):
#     table_name = 'wallet'
#     query = f"SELECT balance FROM {table_name} WHERE account_no = %s LIMIT 1"
#     cursor.execute(query, (bill_reference_number,))
#     balance = cursor.fetchone()
#     return balance[0] if balance else None

def fetch_balance_from_paygo_db(bill_reference_number, cursor):
    table_name = 'walletvalidation'
    query = f"SELECT balance, device_id FROM {table_name} WHERE account_no = %s LIMIT 1"
    cursor.execute(query, (bill_reference_number,))
    balance_and_device_id = cursor.fetchone()
    return balance_and_device_id if balance_and_device_id else (None, None)

# function to fetch balance from the paygo database and update the balance column
def update_balance_in_paygo_db(bill_reference_number, new_wallet_balance, cursor):
    table_name = 'walletvalidation'
    query = f"UPDATE {table_name} SET balance = %s WHERE account_no = %s"
    cursor.execute(query, (new_wallet_balance, bill_reference_number))
    cursor.connection.commit()

def fetch_device_details(device_id, cursor):
    table_name = 'paygoproduction'
    query = f"SELECT daily_rate,expiry_date,serial_no FROM {table_name} WHERE id = %s"
    cursor.execute(query, (device_id,))
    device_details = cursor.fetchone()
    return device_details if device_details else None


def update_device_details_in_paygo_db(device_id, validation_daily_rate, expiry_due_dates, cursor):
    table_name = 'paygoproduction'
    query = f"UPDATE {table_name} SET daily_rate = %s, expiry_date = %s WHERE id = %s"
    cursor.execute(query, (validation_daily_rate, expiry_due_dates, device_id))
    cursor.connection.commit()

cursor_payments.close()

connection_payments.close()


process_bill_reference_numbers(rows)

cursor_paygo.close()
connection_paygo.close()
cursor_validation.close()