import gspread
from google.oauth2.service_account import Credentials
import requests
import json
from hashlib import md5
from bs4 import BeautifulSoup, Comment
import time
import re
import csv
import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time

# Define the scopes
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load Google credentials with the appropriate scopes
creds = Credentials.from_service_account_file('C:/Users/Admin/Desktop/key.json', scopes=scopes)
gspread_client = gspread.authorize(creds)

# Use the URL to open the Google Sheet
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1atWzQLjZ63EzrYJYZ4IcEp_QE3yPHMpNM_e63KmeF6A/edit'  # Replace with your spreadsheet URL

# Mailchimp configuration

list_id = 'be61767c08'
server_prefix = 'us5'
base_url = f'https://{server_prefix}.api.mailchimp.com/3.0'
headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

# Function to check if a contact exists in Mailchimp
def check_contact_exists(email):
    email_hash = md5(email.lower().encode('utf-8')).hexdigest()
    url = f'{base_url}/lists/{list_id}/members/{email_hash}'
    response = requests.get(url, headers=headers)
    return response.status_code == 200

# Function to add a contact to Mailchimp
def add_contact(email, company_name, address, phone_number, tag, link):
    data = {
        'email_address': email,
        'status': 'subscribed',
        'merge_fields': {
            'COMPANY': company_name,
            'ADDRESS': address,
            'PHONE': phone_number,
            'WEBSITE': link
        },
        'tags': [tag]
    }
    
    response = requests.post(f'{base_url}/lists/{list_id}/members', headers=headers, data=json.dumps(data))
    return response

# Function to send a subscription invitation email in Mailchimp
def resubscribe_contact(email, company_name, address, phone_number, tag, link):
    email_hash = md5(email.lower().encode('utf-8')).hexdigest()
    url = f'{base_url}/lists/{list_id}/members/{email_hash}'
    data = {
        'status_if_new': 'pending',
        'email_address': email,
        'merge_fields': {
            'COMPANY': company_name,
            'ADDRESS': address,
            'PHONE': phone_number,
            'WEBSITE': link
        },
        'tags': [tag]
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    return response

# Function to validate an email address
def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None and ' ' not in email


# Improved function to get HTML and check for AMFG iframe on a company website
def get_html(url, timeout=20):
    try:
        response = requests.get(url, timeout=timeout)
        return response.text if response.status_code == 200 else None
    except requests.exceptions.RequestException as e:
        print(f'Error fetching page {url}: {e}')
        return None

# Function to extract email and AMFG status from a webpage
def get_email_and_amfg_status(url):
    html = get_html(url)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        emails = set()
        
        # Find emails in mailto links
        mailto_links = soup.select('a[href^=mailto]')
        for link in mailto_links:
            mailto_email = link['href'].replace('mailto:', '').strip()
            if is_valid_email(mailto_email):
                emails.add(mailto_email)
        
        # Find email addresses in text and attributes
        potential_emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", html)
        for email in potential_emails:
            if is_valid_email(email):
                emails.add(email)

        # Check for AMFG iframe
        has_amfg_iframe = bool(soup.find('iframe', src=lambda x: x and 'amfg' in x))
        
        # Select first valid email or return '??' if none found
        email = next(iter(emails), '??')

        return email, has_amfg_iframe
    return None, False


# Function to extract email from JSON-LD
def extract_email_from_json_ld(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'email' and isinstance(value, str):
                if is_valid_email(value):
                    return value
            elif isinstance(value, dict):
                email = extract_email_from_json_ld(value)
                if email:
                    return email
            elif isinstance(value, list):
                for item in value:
                    email = extract_email_from_json_ld(item)
                    if email:
                        return email
    return None
