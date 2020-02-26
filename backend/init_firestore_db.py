import csv
import os
import requests

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

dir_path = os.path.dirname(os.path.realpath(__file__))

FIRESTORE_PRIVATE_KEY = f"{dir_path}/db/serviceAccountCredentials.json"
# FIRESTORE_DATA_CSV = f"{dir_path}/db/StarterCodeSampleData.csv"

data_arr = [
    'locationCountry',
    'merchantCategoryCode',
    'merchantName',
    'currencyAmount',
    'locationPostalCode',
    'customerId',
    'merchantId',
    'locationStreet'
]

def init_firebase():

    """
    Gets 1000 customers from TD API and dump into firebase
    """
    firestore_client = init_firestore()

    auth_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJDQlAiLCJ0ZWFtX2lkIjoiYTUxNDUwZDUtY2M3NC0zY2E2LWFiNjUtYzY4MGUyMTk5OWE4IiwiZXhwIjo5MjIzMzcyMDM2ODU0Nzc1LCJhcHBfaWQiOiIyMzExZWFkMi1jMjVmLTRiYzMtYjA1Zi1jYjZiMzU0ODE2NDQifQ.2k8mwOqeDvhM9AiARrZdCBL2jjERmPEQcWvKgs7AcRY'
    
    # Get 1000 customers from TD API
    response = requests.post('https://api.td-davinci.com/api/raw-customer-data',
		headers = { 'Authorization': auth_key })
    response_data = response.json()
    
    # iterate through customers 
    for i in range(100):
        transaction_info = {}

        customer_id = response_data['result']['customers'][i]['id']
        
        get_response = requests.get('https://api.td-davinci.com/api/customers/' + customer_id + '/transactions',
			headers = {'Authorization': auth_key})
        cust_transactions = get_response.json()
        
        # interate through transactions 
        for j in range(100):
            record = firestore_client.collection('bank_customers').document(customer_id).collection('transactions').document(str(j))

            # get all transaction info
            for k in range(len(data_arr)):
                transaction_info.update( {data_arr[k] : cust_transactions.get('result')[j].get(data_arr[k]) } )
          
            record.set(transaction_info)

def init_firestore():

    cred = credentials.Certificate(FIRESTORE_PRIVATE_KEY)
    firebase_admin.initialize_app(cred)

    return firestore.client()

if __name__ == "__main__":
    init_firebase()
