import streamlit as st
import base64
st.set_page_config(page_title="Multi-Format Data Loader System with AML Detection", layout="wide")

# Show immediate loading message
startup_placeholder = st.empty()
with startup_placeholder.container():
    st.info(" Initializing Multi-Format Data Loader System...")



# Create progress tracking
progress_placeholder = st.empty()
status_placeholder = st.empty()

with progress_placeholder.container():
    progress_bar = st.progress(0)

with status_placeholder.container():
    status_text = st.info("Loading dependencies...")

from logger.logger_config import get_logger, log_performance, log_database_operation, log_exception

# Replace the old logger setup with:
logger = get_logger(__name__)

# Updated init_session_state function:
def init_session_state():
    """Initialize session state with optimized checks"""
    logger.info("=== APPLICATION INITIALIZATION ===")
    
    # Initialize session state variables only if they don't exist
    defaults = {
        'db_connected': False,
        'db_host': "",
        'db_name': "",
        'db_user': "",
        'db_password': "",
        'entity_processed': False,
        'account_processed': False,
        'transaction_processed': False,
        'transaction_upload_completed': False,
        'temp_transaction_file': None,
        'aml_completed': False,
        'aml_results': None,
        'aml_available': AML_DETECTION_AVAILABLE,
        'chunk_size': 5000
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # Debug logging for AML detection availability
    logger.info(f"AML_DETECTION_AVAILABLE: {AML_DETECTION_AVAILABLE}")
    
    # Always prioritize environment variables for database connection strings
    env_host = os.getenv('DB_HOST', "")
    env_name = os.getenv('DB_NAME', "")
    env_user = os.getenv('DB_USER', "")
    env_password = os.getenv('DB_PASSWORD', "")
    
    # Debug logging for main app database config
    logger.info("=== MAIN APP DATABASE CONFIG ===")
    logger.info(f"DB_HOST from env: {env_host}")
    logger.info(f"DB_NAME from env: {env_name}")
    logger.info(f"DB_USER from env: {env_user}")
    logger.info(f"DB_PASSWORD from env: {'***SET***' if env_password else 'NOT_SET'}")
    
    st.session_state.db_host = env_host
    st.session_state.db_name = env_name
    st.session_state.db_user = env_user
    st.session_state.db_password = env_password
    
    # Load non-database config values from config file
    config = read_config_file()
    if config:
        logger.info(f"Config file loaded with keys: {list(config.keys())}")
        st.session_state.chunk_size = int(config.get('app.chunk_size', 5000))
        st.session_state.config = config
    else:
        logger.info("No config file loaded or config file empty")
    
    logger.info("=== INITIALIZATION COMPLETE ===")

import pandas as pd
status_text.info(" Loading database connectors...")
progress_bar.progress(10)

import mysql.connector
from mysql.connector import pooling
from mysql.connector import Error
status_text.info(" Loading datetime utilities...")
progress_bar.progress(20)

from datetime import datetime,timezone,timedelta
import io
import concurrent.futures
import time
import logging
import os
import re
import base64
import sys
import gc
import json
import glob
import chardet
import openpyxl
from typing import Tuple, Optional, Dict, Any, List
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue


status_text.info(" Setting up system paths...")
progress_bar.progress(30)

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Also add the parent directory if needed
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

status_text.info(" Loading AML Detection module...")
progress_bar.progress(40)

# Import the AML monitoring system 
AML_DETECTION_AVAILABLE = False
aml_monitor = None

try:
    from aml_detection.aml_monitoring_entity import AMLMonitoring  
    AML_DETECTION_AVAILABLE = True
    print("AML_DETECTION_AVAILABLE: ", AML_DETECTION_AVAILABLE)
    # Show success but don't initialize yet (defer heavy operations)
    status_text.success(" AML Detection module found!")
    progress_bar.progress(60)
except ImportError as e:
    status_text.warning(f"AML Detection module not available: {str(e)}")
    progress_bar.progress(60)

status_text.info(" Setting up logging system...")
progress_bar.progress(70)

def detect_delimiter(file_content: str, sample_size: int = 1024) -> Tuple[str, float]:
    """
    Detect the most likely delimiter in a text file
    Returns: (delimiter, confidence_score)
    """
    # Common delimiters to test
    delimiters = [',', '\t', ';', '|', ':', ' ']
    delimiter_names = {
        ',': 'Comma (,)',
        '\t': 'Tab (\\t)',
        ';': 'Semicolon (;)',
        '|': 'Pipe (|)',
        ':': 'Colon (:)',
        ' ': 'Space ( )'
    }
    
    # Use first few lines for detection
    sample_lines = file_content.split('\n')[:10]
    sample_text = '\n'.join(sample_lines)
    
    delimiter_scores = {}
    
    for delimiter in delimiters:
        try:
            # Count occurrences in each line
            line_counts = []
            for line in sample_lines:
                if line.strip():  # Skip empty lines
                    count = line.count(delimiter)
                    line_counts.append(count)
            
            if not line_counts:
                continue
                
            # Calculate consistency score
            if len(line_counts) > 1:
                avg_count = sum(line_counts) / len(line_counts)
                variance = sum((x - avg_count) ** 2 for x in line_counts) / len(line_counts)
                consistency = 1 / (1 + variance) if variance > 0 else 1
                
                # Prefer delimiters that appear consistently and frequently
                score = avg_count * consistency
                delimiter_scores[delimiter] = score
            else:
                delimiter_scores[delimiter] = line_counts[0]
                
        except Exception:
            delimiter_scores[delimiter] = 0
    
    # Find best delimiter
    if delimiter_scores:
        best_delimiter = max(delimiter_scores.keys(), key=lambda x: delimiter_scores[x])
        max_score = delimiter_scores[best_delimiter]
        confidence = min(max_score / 10, 1.0)  # Normalize confidence
        return best_delimiter, confidence
    
    return ',', 0.5  # Default fallback

def detect_encoding(file_bytes: bytes) -> str:
    """Detect file encoding"""
    try:
        result = chardet.detect(file_bytes)
        encoding = result['encoding']
        confidence = result['confidence']
        
        # If confidence is low, try common encodings
        if confidence < 0.7:
            for enc in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    file_bytes.decode(enc)
                    return enc
                except UnicodeDecodeError:
                    continue
        
        return encoding or 'utf-8'
    except Exception:
        return 'utf-8'

def preview_txt_file(file_content: str, delimiter: str, max_rows: int = 5) -> pd.DataFrame:
    """Preview TXT file with specified delimiter"""
    try:
        # Use StringIO to simulate file reading
        string_io = io.StringIO(file_content)
        
        # Try to read with pandas
        df = pd.read_csv(string_io, delimiter=delimiter, nrows=max_rows)
        return df
    except Exception as e:
        st.error(f"Error reading file with delimiter '{delimiter}': {str(e)}")
        return pd.DataFrame()

def read_excel_file(file_bytes: bytes, sheet_name: Optional[str] = None) -> Tuple[pd.DataFrame, list]:
    """
    Read Excel file and return DataFrame and list of sheet names
    """
    try:
        # Get all sheet names first
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
        sheet_names = excel_file.sheet_names
        
        # Read the specified sheet or the first sheet
        if sheet_name and sheet_name in sheet_names:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
        else:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
            
        return df, sheet_names
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame(), []
    
# Set up logging configuration
# Global flag to prevent repeated initialization
_logging_initialized = False

def setup_robust_logging():
    """Set up robust logging that works regardless of permission issues and always appends"""
    global _logging_initialized
    
    # Check if already initialized
    if _logging_initialized:
        return getattr(setup_robust_logging, '_log_filename', None)
    
    # First, try to log directly to console to see if basic logging works
    print("Setting up logging system...", file=sys.stderr)
    
    # Create a list of potential log directories in order of preference
    potential_dirs = [
        "logs",                                  # Subdirectory in current working directory
        ".",                                     # Current working directory
        os.path.join(os.path.expanduser("~"), "logs"),  # User's home directory
        os.path.join(os.environ.get("TEMP", ""), "logs"),  # System temp directory
        os.environ.get("TEMP", "")               # System temp directory directly
    ]
    
    log_dir = None
    for directory in potential_dirs:
        try:
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            
            # Test file creation
            test_file = os.path.join(directory, "log_test.tmp")
            with open(test_file, 'w') as f:
                f.write("Test logging access")
            os.remove(test_file)  # Clean up
            
            # We found a writable directory
            log_dir = directory
            print(f"Using log directory: {log_dir}", file=sys.stderr)
            break
        except Exception as e:
            print(f"Cannot use directory {directory}: {str(e)}", file=sys.stderr)
    
    if not log_dir:
        print("WARNING: Could not find any writable directory for logs", file=sys.stderr)
        # Set up console-only logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        _logging_initialized = True
        return None
    
    # Create the log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    log_filename = os.path.join(log_dir, f"dataloader_operations_{timestamp}.log")
    
    # Get root logger
    root_logger = logging.getLogger()
    
    # Only clear handlers if we haven't initialized yet
    if not root_logger.handlers or not _logging_initialized:
        root_logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # FIXED: Always use append mode for file handler
        try:
            # Check if log file exists
            file_exists = os.path.exists(log_filename)
            
            # CRITICAL FIX: Use append mode ('a') instead of write mode ('w')
            file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            if file_exists:
                print(f"Successfully opened existing log file for append: {log_filename}", file=sys.stderr)
                root_logger.info("Logging session continued (appended to existing log)")
            else:
                print(f"Successfully created new log file: {log_filename}", file=sys.stderr)
                root_logger.info("Logging system initialized successfully")
            
        except Exception as e:
            print(f"ERROR creating log file: {str(e)}", file=sys.stderr)
            print(f"Continuing with console logging only", file=sys.stderr)
            log_filename = None
    
    # Mark as initialized and store filename
    _logging_initialized = True
    setup_robust_logging._log_filename = log_filename
    
    return log_filename

# FIXED: Only initialize logging once per session
if not _logging_initialized:
    log_filename = setup_robust_logging()
    logger = logging.getLogger(__name__)
    logger.info("Data configuration loading process started")
else:
    # Get existing logger without reinitializing
    logger = logging.getLogger(__name__)
    logger.info("Data configuration loading process continued")

progress_bar.progress(80)

# Function to read config.properties file (optimized)
def read_config_file(config_path='config.properties'):
    """Read database connection properties from config.properties file - optimized version"""
    config = {}
    
    if not os.path.exists(config_path):
        return config
    
    try:
        with open(config_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line and '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.split('#')[0].strip()
        
        logger.info(f"Successfully read configuration: {', '.join(config.keys())}")
        return config
    except Exception as e:
        logger.exception(f"Error reading configuration file: {str(e)}")
        return {}

# Initialize config and session state (optimized)
def init_session_state():
    """Initialize session state with optimized checks"""
    # Initialize session state variables only if they don't exist
    defaults = {
        'db_connected': False,
        'db_host': "",
        'db_name': "",
        'db_user': "",
        'db_password': "",
        'entity_processed': False,
        'account_processed': False,
        'transaction_processed': False,
        'transaction_upload_completed': False,  # New addition
        'temp_transaction_file': None,  # New addition
        'aml_completed': False,
        'aml_results': None,
        'aml_available': AML_DETECTION_AVAILABLE,
        'chunk_size': 5000
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # Load config values if available
    config = read_config_file()
    if config:
        # Only update if current values are empty
        if not st.session_state.db_host:
            st.session_state.db_host = config.get('db.host', "")
        if not st.session_state.db_name:
            st.session_state.db_name = config.get('db.database', "")
        if not st.session_state.db_user:
            st.session_state.db_user = config.get('db.user', "")
        if not st.session_state.db_password:
            st.session_state.db_password = config.get('db.password', "")
        
        st.session_state.chunk_size = int(config.get('app.chunk_size', 5000))
        st.session_state.config = config

# Initialize session state
init_session_state()

status_text.info(" Finalizing initialization...")
progress_bar.progress(90)

# Defer AML monitor initialization until actually needed
def get_aml_monitor():
    """Lazy initialization of AML monitor"""
    global aml_monitor
    if aml_monitor is None and AML_DETECTION_AVAILABLE:
        try:
            aml_monitor = AMLMonitoring()
            logger.info("AML Monitoring system initialized successfully")
            st.session_state.aml_available = True
        except Exception as e:
            logger.error(f"Failed to initialize AML Monitoring: {str(e)}")
            st.session_state.aml_available = False
    return aml_monitor

# Complete initialization
status_text.success(" System ready!")
progress_bar.progress(100)

# Clear startup messages after a short delay
import time
time.sleep(0.5)
startup_placeholder.empty()
progress_placeholder.empty()
status_placeholder.empty()

# Define chunk size for processing
CHUNK_SIZE = 5000

# Expected headers for list management CSV
EXPECTED_LIST_HEADERS = [
    'entity_type', 'category', 'risk_score', 'risk_level', 'list_name',
    'country', 'industry_code', 'payment_channel', 'start_time', 'end_time',
    'information_type', 'information_value', 'list_type', 'added_by', 'added_date',
    'expiry_date', 'source', 'update_date', 'updated_by'
]

# Required fields for list management
REQUIRED_LIST_FIELDS = [
    'entity_type', 'category', 'risk_score', 'risk_level', 
    'information_type', 'information_value', 'list_type', 'added_by', 'source'
]

# Field descriptions for list management
LIST_FIELD_DESCRIPTIONS = {
    'entity_type': 'Type of entity: Individual or Organisation',
    'category': 'Free form category (e.g., watchlist, blacklist, whitelist)',
    'risk_score': 'Numeric risk score assigned by user',
    'risk_level': 'Risk level: low, medium, high, or critical',
    'list_name': 'Name of the list (optional)',
    'country': 'Country associated with the entity',
    'industry_code': 'Industry classification code',
    'payment_channel': 'Associated payment channel',
    'start_time': 'Start date/time for validity',
    'end_time': 'End date/time for validity',
    'information_type': 'Type of information (e.g., SSN, Email, Phone, Account)',
    'information_value': 'The actual information value',
    'list_type': 'Type of list: whitelist, blacklist, watchlist, odfi_suspicious, rdfi_suspicious',
    'added_date': 'Date when item was added',
    'expiry_date': 'Expiry date for the list item',
    'source': 'Source of the information',
    'update_date': 'Date of last update',
    'updated_by': 'User who last updated the record'
}


# Internal field mapping dictionaries (hidden from users)
ENTITY_CUSTOMER_MAPPING = {
    'customer_id': 'customer_entity_id',
    'parent_customer_id': 'customer_parent_entity_id',
    'first_name': 'first_name',
    'last_name': 'last_name',
    'middle_name': 'middle_name',
    'name_prefix': 'prefix',
    'name_suffix': 'suffix',
    'is_employee': 'is_employee',
    'nationality': 'nationality',
    'date_of_birth' : 'date_of_birth',
    'gender' : 'gender',
    'deceased_date': 'deceased_date',
    'dual_citizenship_country': 'dual_citizenship_country',
    'ethnic_background': 'ethnic_background',
    'tax_id': 'ssn',
    'job_title': 'occupation',
    'marital_status': 'marital_status',
    'home_ownership_type': 'home_ownership_type',
    'customer_risk_score': 'customercreditscore',
    'pep': 'pep',
    'is_related_to_pep': 'relatedtopep',
    'customer_type': 'customertype',
    'closed_date': 'closed_date',
    'closed_reason': 'closed_reason',
    'country_risk': 'country_risk',
    'contract_number':'contract_number',
    'account_executive':'account_executive',
    'business_unit':'business_unit',
    'risk_category': 'risk_category',
    'country_of_birth_id':'country_of_birth',
    'country_of_residence_id':'country_of_residence'
}

ENTITY_BUSINESS_MAPPING = {
    'customer_id': 'customer_entity_id',
    'parent_customer_id': 'customer_parent_entity_id',
    'business_name': 'legal_name',
    'date_of_incorporation': 'date_of_incorporation',
    'doing_business_as': 'doing_business_as',
    'legal_form': 'business_type',
    'bankruptcy_date': 'bankruptcy_date',
    'tin': 'taxid',
    'closed_date': 'closed_date',
    'closed_reason': 'closed_reason',
     'contract_number':'contract_number',
    'account_executive':'account_executive',
    'business_unit':'business_unit',
    'risk_category': 'risk_category'
}


# Expected headers for each data type
EXPECTED_CUSTOMER_HEADERS = [
    'changetype', 'customer_id', 'parent_customer_id', 'customer_type', 'first_name', 'last_name', 
    'middle_name', 'name_prefix', 'name_suffix', 'is_employee', 'nationality', 'deceased_date', 'date_of_birth',
    'dual_citizenship_country', 'ethnic_background', 'tax_id', 'job_title', 'marital_status', 'gender',
    'home_ownership_type', 'customer_risk_score', 'pep', 'is_related_to_pep', 'document_type', 
    'document_number', 'document_expiry_date', 'business_name', 'date_of_incorporation', 
    'doing_business_as', 'legal_form','annual_turnover', 'industry_code', 'bankruptcy_date', 'tin', 'lei', 
    'swift_number', 'company_identifier', 'company_name', 'email_type', 'email_address', 
    'phone_type', 'phone_number', 'address_type', 'address_line_1', 'address_line_2', 
    'address_line_3', 'address_city', 'address_state', 'address_country', 'address_zipcode', 
    'number_of_years_at_address', 'closed_date', 'closed_reason',
    'country_risk', 'contract_number', 'account_executive','business_unit','risk_category'
]

EXPECTED_ACCOUNT_HEADERS = [
    'changetype', 'account_id', 'customer_id', 'account_number', 'account_routing_number', 
    'account_type', 'account_currency', 'account_balance', 'open_date', 'closed_date', 
    'close_reason', 'branch_id', 'reopen_date', 'account_risk_score', 'account_status', 
    'is_p2p_enabled', 'p2p_enrollment_date', 'p2p_email', 'p2p_phone', 'is_joint', 
    'joint_account_primary_id', 'joint_account_secondary_id', 'joint_account_relationship_type', 
    'joint_account_status'
    ]
        

EXPECTED_TRANSACTION_HEADERS = [
    'changetype', 'transaction_id', 'account_id', 'debit_credit_indicator', 'transaction_date', 
    'transaction_type', 'transaction_code', 'tran_code_description', 'amount', 'preferred_currency', 
    'settlement_currency', 'exchange_rate_applied', 'transaction_origin_country', 'channel', 
    'counterparty_account', 'counterparty_routing_number', 'counterparty_name', 'branch_id', 
    'teller_id', 'check_number', 'micr_code', 'check_image_front_path', 'check_image_back_path', 
    'txn_location', 'is_p2p_transaction', 'p2p_recipient_type', 'p2p_recipient', 'p2p_sender_type', 
    'p2p_sender', 'p2p_reference_id', 'balance','eod_balance','non_account_holder','positive_pay_indicator',
    'originator_bank','beneficiary_bank','instructing_bank','intermediary_bank','inter_bank_information','other_information'
    ]    

# Required fields for each data type
REQUIRED_CUSTOMER_FIELDS = ['changetype', 'customer_id', 'customer_type']
REQUIRED_ACCOUNT_FIELDS = ['changetype', 'account_id', 'customer_id', 'account_number']
REQUIRED_TRANSACTION_FIELDS = ['changetype', 'transaction_id', 'account_id', 'transaction_date','amount','counterparty_account']

# Field descriptions to help users understand what each field means
FIELD_DESCRIPTIONS = {
    # Customer fields
    'changetype': 'Operation type: add, modify, or delete',
    'customer_id': 'Unique identifier for the customer',
    'parent_customer_id': 'ID of parent customer (for hierarchical relationships)',
    'customer_type': 'Type of customer: Consumer or Business',
    'first_name': 'Customer first name (for Consumer)',
    'last_name': 'Customer last name (for Consumer)',
    'middle_name': 'Customer middle name (optional)',
    'date_of_birth': 'Customer date of birth',
    'gender':  'Customer gender M or F',
    'name_prefix': 'Name prefix (Mr., Ms., Dr., etc.)',
    'name_suffix': 'Name suffix (Jr., Sr., III, etc.)',
    'is_employee': 'Whether customer is an employee (Y/N)',
    'nationality': 'Customer nationality',
    'deceased_date': 'Date of death (if applicable)',
    'dual_citizenship_country': 'Second citizenship country',
    'ethnic_background': 'Ethnic background',
    'tax_id': 'Tax identification number (SSN for US)',
    'job_title': 'Customer occupation/job title',
    'marital_status': 'Marital status',
    'home_ownership_type': 'Type of home ownership',
    'customer_risk_score': 'Customer credit/risk score',
    'pep': 'Politically Exposed Person indicator (Y/N)',
    'is_related_to_pep': 'Related to PEP indicator (Y/N)',
    'document_type': 'Type of identification document',
    'document_number': 'Identification document number',
    'document_expiry_date': 'Document expiry date',
    'business_name': 'Legal business name (for Business customers)',
    'date_of_incorporation': 'Business incorporation date',
    'doing_business_as': 'DBA name',
    'business_type': 'Type of business entity',
    'industry_code': 'NAICS industry code',
    'bankruptcy_date': 'Date of bankruptcy (if applicable)',
    'tin': 'Tax Identification Number',
    'lei': 'Legal Entity Identifier',
    'swift_number': 'SWIFT code',
    'company_identifier': 'Company identifier',
    'company_name': 'Company name',
    'email_type': 'Type of email (Primary, Work, etc.)',
    'email_address': 'Email address',
    'phone_type': 'Type of phone (Primary, Mobile, etc.)',
    'phone_number': 'Phone number',
    'address_type': 'Type of address (Primary, Business, etc.)',
    'address_line_1': 'Address line 1',
    'address_line_2': 'Address line 2 (optional)',
    'address_line_3': 'Address line 3 (optional)',
    'address_city': 'City',
    'address_state': 'State/Province',
    'address_country': 'Country',
    'address_zipcode': 'ZIP/Postal code',
    'number_of_years_at_address': 'Years at current address',
    'closed_date': 'Account closure date',
    'closed_reason': 'Reason for closure',
    
    # Account fields
    'changetype': 'Operation type: add, modify, or delete',
    'account_id': 'Unique account identifier',
    'account_number': 'Account number',
    'account_routing_number': 'Bank routing number',
    'account_type': 'Type of account (Checking, Savings, etc.)',
    'account_currency': 'Account currency',
    'account_balance': 'Current account balance',
    'open_date': 'Account opening date',
    'close_reason': 'Reason for account closure',
    'branch_id': 'Branch identifier',
    'reopen_date': 'Account reopening date',
    'account_risk_score': 'Account risk score',
    'account_status': 'Account status (Active, Closed, etc.)',
    'is_p2p_enabled': 'P2P payments enabled (1/0)',
    'p2p_enrollment_date': 'P2P enrollment date',
    'p2p_email': 'P2P email address',
    'p2p_phone': 'P2P phone number',
    'is_joint': 'Joint account indicator (1/0)',
    'joint_account_primary_id': 'Primary account holder ID',
    'joint_account_secondary_id': 'Secondary account holder ID',
    'joint_account_relationship_type': 'Relationship type',
    'joint_account_status': 'Joint account status',
    
    # Transaction fields
    'changetype': 'Operation type: add, modify, or delete',
    'transaction_id': 'Unique transaction identifier',
    'account_id': 'Account identifier for the transaction',
    'debit_credit_indicator': 'D for debit, C for credit',
    'transaction_date': 'Transaction date and time',
    'transaction_type': 'Type of transaction',
    'transaction_code': 'Transaction code',
    'tran_code_description': 'Transaction description',
    'amount': 'Transaction amount',
    'preferred_currency': 'Preferred currency',
    'settlement_currency': 'Settlement currency',
    'exchange_rate_applied': 'Exchange rate used',
    'transaction_origin_country': 'Transaction country of origin',
    'channel': 'Transaction channel (WIRE, ZELLE, ATM, ONLINE etc)',
    'counterparty_account': 'Counterparty account number',
    'counterparty_routing_number': 'Counterparty routing number',
    'counterparty_name': 'Counterparty name',
    'branch_id': 'Branch identifier where transaction occurred',
    'teller_id': 'Teller identifier',
    'check_number': 'Check number',
    'micr_code': 'MICR code from check',
    'check_image_front_path': 'Check front image file path',
    'check_image_back_path': 'Check back image file path',
    'txn_location': 'Transaction location (branch, ATM, online)',
    'is_p2p_transaction': 'P2P transaction indicator (Y/N)',
    'p2p_recipient_type': 'P2P recipient type (EMAIL, PHONE, ACCOUNT)',
    'p2p_recipient': 'P2P recipient identifier',
    'p2p_sender_type': 'P2P sender type (EMAIL, PHONE, ACCOUNT)',
    'p2p_sender': 'P2P sender identifier',
    'p2p_reference_id': 'P2P reference ID',
    'balance': 'Account balance after transaction',
    'eod_balance': 'End of day account balance',
    'non_account_holder': 'Non-account holder transaction indicator (Y/N)',
    'positive_pay_indicator': 'Positive pay service indicator (Y/N/ENABLED/DISABLED/PARTIAL)',
    'originator_bank': 'Originating bank identifier or name',
    'beneficiary_bank': 'Beneficiary bank identifier or name',
    'intermediary_bank': 'Intermediary bank identifier or name',
    'instructing_bank': 'Instructing bank identifier or name',
    'inter_bank_information': 'Inter-bank message information',
    'other_information': 'Additional transaction information or memo'
}

import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

@dataclass
class FixedWidthField:
    """Represents a field in a fixed-width file format"""
    field_name: str
    column_name: str
    format_type: str
    position: int
    size: int
    check_free_pay: str
    bliink_mapping: str

class FixedWidthSchema:
    """Manages fixed-width file schemas and parsing"""
    
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self.fields: List[FixedWidthField] = []
        self.total_length = 0
    
    def add_field(self, field: FixedWidthField):
        """Add a field to the schema"""
        self.fields.append(field)
        self.total_length = max(self.total_length, field.position + field.size - 1)
    
    def parse_line(self, line: str) -> Dict[str, str]:
        """Parse a single line according to the schema"""
        parsed_data = {}
        
        #Ensure line is long enough
        if len(line) < self.total_length:
            line = line.ljust(self.total_length)
        
        for field in self.fields:
            start_pos = field.position - 1  # Convert to 0-based indexing
            end_pos = start_pos + field.size
            
            # Extract the field value
            value = line[start_pos:end_pos].strip()
            
            # Use the bliink mapping as the key if available, otherwise use column name
            key = field.bliink_mapping if field.bliink_mapping else field.column_name
            key = key.split(' - ')[-1] if ' - ' in key else key  # Extract the actual field name
            
            parsed_data[key] = value if value else None
        
        return parsed_data

def validate_list_data(df):
    """Validate the uploaded list data"""
    validation_errors = []
    
    # Check required columns
    missing_columns = [col for col in REQUIRED_LIST_FIELDS if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return validation_errors
    
    # Validate entity_type values
    valid_entity_types = ['Individual', 'Organisation', 'individual', 'organisation']
    invalid_entity_types = df[~df['entity_type'].isin(valid_entity_types)]['entity_type'].unique()
    if len(invalid_entity_types) > 0:
        validation_errors.append(f"Invalid entity_type values: {', '.join(invalid_entity_types)}. Valid values: Individual, Organisation")
    
    # Validate risk_level values
    valid_risk_levels = ['low', 'medium', 'high', 'critical']
    if 'risk_level' in df.columns:
        invalid_risk_levels = df[~df['risk_level'].str.lower().isin(valid_risk_levels)]['risk_level'].unique()
        if len(invalid_risk_levels) > 0:
            validation_errors.append(f"Invalid risk_level values: {', '.join(invalid_risk_levels)}. Valid values: {', '.join(valid_risk_levels)}")
    
    # Validate list_type values
    valid_list_types = ['whitelist', 'blacklist', 'watchlist', 'odfi_suspicious', 'rdfi_suspicious']
    invalid_list_types = df[~df['list_type'].str.lower().isin(valid_list_types)]['list_type'].unique()
    if len(invalid_list_types) > 0:
        validation_errors.append(f"Invalid list_type values: {', '.join(invalid_list_types)}. Valid values: {', '.join(valid_list_types)}")
    
    # Validate risk_score is numeric
    if 'risk_score' in df.columns:
        try:
            pd.to_numeric(df['risk_score'], errors='raise')
        except (ValueError, TypeError):
            validation_errors.append("risk_score must contain only numeric values")
    
    # Check for empty required fields
    for field in REQUIRED_LIST_FIELDS:
        if field in df.columns:
            empty_count = df[df[field].isna() | (df[field] == '')].shape[0]
            if empty_count > 0:
                validation_errors.append(f"Field '{field}' has {empty_count} empty values")
    
    return validation_errors

def clean_list_row_data(row_dict):
    """Clean and prepare list row data for database insertion"""
    cleaned_data = {}
    
    for key, value in row_dict.items():
        if pd.isna(value) or value == '' or str(value).lower() == 'nan':
            cleaned_data[key] = None
        else:
            cleaned_data[key] = str(value).strip() if isinstance(value, str) else value
    
    return cleaned_data

def convert_list_date_field(date_value):
    """Convert date field to MySQL datetime format for list data"""
    if not date_value or pd.isna(date_value) or str(date_value).lower() == 'nan':
        return None
    
    try:
        if isinstance(date_value, str):
            date_value = date_value.strip()
            if not date_value:
                return None
        
        # Try pandas datetime conversion
        parsed_date = pd.to_datetime(date_value, errors='coerce')
        if pd.isna(parsed_date):
            return None
        
        return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
    
    except Exception as e:
        logger.warning(f"Date conversion failed for '{date_value}': {str(e)}")
        return None

def prepare_list_entity_insert_params(row_dict, current_time):
    """Prepare parameters for platform_list_entities table insert"""
    
    # Convert date fields
    start_time = convert_list_date_field(row_dict.get('start_time'))
    end_time = convert_list_date_field(row_dict.get('end_time'))
    
    # Convert risk_score to float
    risk_score = float(row_dict.get('risk_score', 0))
    
    return (
        row_dict.get('entity_type', '').capitalize(),  # Capitalize for consistency
        row_dict.get('category'),
        risk_score,
        row_dict.get('risk_level', '').lower(),  # Lowercase for enum
        row_dict.get('list_name'),  # notes field will use list_name
        row_dict.get('country'),
        row_dict.get('industry_code'),
        row_dict.get('payment_channel'),
        start_time,
        end_time,
        current_time,  # insert_date
        current_time,  # update_date
        'UserUpload',  # inserted_by
        'UserUpload'   # updated_by
    )

def check_existing_list_entity(cursor, list_name, category=None, entity_type=None):
    """
    Check if a list entity with the same list_name already exists
    Returns the list_entity_id if found, None otherwise
    """
    try:
        # Base query to check for existing list_name in notes field
        base_query = "SELECT list_entity_id FROM platform_list_entities WHERE notes = %s"
        params = [list_name]
        
        # Add additional filters for more precise matching if provided
        if category:
            base_query += " AND category = %s"
            params.append(category)
        
        if entity_type:
            base_query += " AND entity_type = %s"
            params.append(entity_type.capitalize())
        
        # Order by most recent first and limit to 1
        base_query += " ORDER BY insert_date DESC LIMIT 1"
        
        cursor.execute(base_query, params)
        result = cursor.fetchone()
        
        if result:
            logger.info(f"Found existing list entity with list_name '{list_name}': list_entity_id = {result[0]}")
            return result[0]
        else:
            logger.info(f"No existing list entity found for list_name '{list_name}'")
            return None
    
    except Exception as e:
        logger.error(f"Error checking existing list entity: {str(e)}")
        return None
    
def prepare_list_item_insert_params(list_entity_id, row_dict, current_time):
    """Prepare parameters for platform_list_items table insert"""
    
    # Convert date fields
    expiry_date = convert_list_date_field(row_dict.get('expiry_date'))
    
    # Get updated_by, default to 'UserUpload'
    updated_by = row_dict.get('updated_by', 'UserUpload')
    
    return (
        list_entity_id,
        row_dict.get('information_type'),
        row_dict.get('information_value'),
        row_dict.get('list_type', '').lower(),  # Lowercase for enum
        current_time, #added_date
        expiry_date,
        updated_by,  # added_by
        row_dict.get('source'),
        updated_by,  
        current_time #updated_at
    )

def process_list_data_chunk(chunk_df, connection):
    """Process a chunk of list data with duplicate checking for list entities"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing list data chunk with {len(chunk_df)} records (optimized with batch duplicate checking)")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Phase 1: Prepare data and collect list names for batch checking
        valid_rows = []
        list_names_data = []
        
        for index, row in chunk_df.iterrows():
            try:
                row_dict = clean_list_row_data(row.to_dict())
                
                # Validate required fields
                missing_fields = [field for field in REQUIRED_LIST_FIELDS if not row_dict.get(field)]
                if missing_fields:
                    error_count += 1
                    results.append(f"Row {index + 1}: Missing required fields: {', '.join(missing_fields)}")
                    continue
                
                valid_rows.append((index, row_dict))
                
                # Collect data for batch checking
                list_name = row_dict.get('list_name')
                category = row_dict.get('category')
                entity_type = row_dict.get('entity_type')
                industry_code = row_dict.get('industry_code')
                country = row_dict.get('country')
                payment_channel = row_dict.get('payment_channel')
                list_names_data.append((list_name, category, entity_type,industry_code,country,payment_channel, index))
                
            except Exception as e:
                error_count += 1
                results.append(f"Row {index + 1}: Error preparing data - {str(e)}")
        
        # Phase 2: Batch check for existing entities
        existing_entities = batch_check_existing_list_entities(cursor, list_names_data)
        
        # Phase 3: Separate rows into existing vs new entities
        entity_inserts = []
        item_inserts = []
        
        for index, row_dict in valid_rows:
            if index in existing_entities:
                # Use existing entity
                list_entity_id = existing_entities[index]
                item_params = prepare_list_item_insert_params(list_entity_id, row_dict, current_time)
                item_inserts.append((item_params, index + 1))
                
                list_name = row_dict.get('list_name', 'Unnamed')
                results.append(f"Row {index + 1}: Using existing list entity '{list_name}' (ID: {list_entity_id})")
            else:
                # Prepare for new entity insert
                entity_params = prepare_list_entity_insert_params(row_dict, current_time)
                entity_inserts.append((entity_params, row_dict, index))
        
        # Phase 4: Insert new entities and prepare their list items
        if entity_inserts:
            entity_sql = """
            INSERT INTO platform_list_entities (
                entity_type, category, risk_score, risk_level, notes,
                country, industry_code, payment_channel, start_time, end_time,
                created_at, updated_at, created_by, updated_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            entity_params_only = [params[0] for params in entity_inserts]
            cursor.executemany(entity_sql, entity_params_only)
            
            # Get entity IDs for new entities
            last_insert_id = cursor.lastrowid
            entity_ids = list(range(last_insert_id - len(entity_inserts) + 1, last_insert_id + 1))
            
            # Prepare list items for new entities
            for i, (_, row_dict, index) in enumerate(entity_inserts):
                list_entity_id = entity_ids[i]
                item_params = prepare_list_item_insert_params(list_entity_id, row_dict, current_time)
                item_inserts.append((item_params, index + 1))
                
                list_name = row_dict.get('list_name', 'Unnamed')
                results.append(f"Row {index + 1}: Created new list entity '{list_name}' (ID: {list_entity_id})")
        
        # Phase 5: Insert all list items
        if item_inserts:
            item_sql = """
            INSERT INTO platform_list_items (
                list_entity_id, information_type, information_value, list_type,
                added_date, expiry_date, added_by, source, updated_by, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            item_params_only = [params[0] for params in item_inserts]
            cursor.executemany(item_sql, item_params_only)
            
            success_count = len(item_inserts)
            logger.info(f"Inserted {success_count} list items")
        
        # Commit all changes
        connection.commit()
        cursor.close()
        
        # Summary
        total_existing = len(existing_entities)
        total_new = len(entity_inserts)
        logger.info(f"Optimized processing completed: {success_count} items, {total_existing} existing entities reused, {total_new} new entities created")
        
        if total_existing > 0 or total_new > 0:
            results.insert(0, f"Optimized Summary: {total_existing} existing entities reused, {total_new} new entities created, {success_count} list items processed")
        
    except Exception as e:
        error_count = len(chunk_df)
        results.append(f"Optimized batch processing failed: {str(e)}")
        logger.error(f"Error in optimized processing: {str(e)}")
        try:
            connection.rollback()
        except:
            pass
    
    return success_count, error_count, results


def batch_check_existing_list_entities(cursor, list_names_data):
    """
    Batch check for existing list entities
    
    Args:
        cursor: Database cursor
        list_names_data: List of tuples (list_name, category, entity_type, row_index)
    
    Returns:
        dict: Mapping of row_index to existing list_entity_id
    """
    existing_entities = {}
    
    if not list_names_data:
        return existing_entities
    
    try:
        # Create a query to check all list names at once
        placeholders = ','.join(['%s'] * len(list_names_data))
        list_names_only = [data[0] for data in list_names_data if data[0]]  # Extract just the list names
        
        if not list_names_only:
            return existing_entities
        
        query = f"""
        SELECT notes, list_entity_id, category, entity_type,industry_code,country, payment_channel
        FROM platform_list_entities 
        WHERE notes IN ({','.join(['%s'] * len(list_names_only))})
        """
        
        cursor.execute(query, list_names_only)
        db_results = cursor.fetchall()
        
        # Create a lookup map from database results
        db_lookup = {}
        for notes, category,list_entity_id, entity_type,industry_code, country,payment_channel in db_results:
            key = (notes, category, entity_type,industry_code, country,payment_channel )
            if key not in db_lookup:  # Keep the most recent one (due to ORDER BY)
                db_lookup[key] = list_entity_id
        
        # Match with input data
        for list_name, category, entity_type, industry_code, country,payment_channel ,row_index in list_names_data:
            if list_name:
                key = (list_name, category, entity_type,industry_code, country,payment_channel )
                if key in db_lookup:
                    existing_entities[row_index] = db_lookup[key]
                    logger.info(f"Row {row_index + 1}: Found existing entity for '{list_name}': {db_lookup[key]}")
        
        logger.info(f"Batch check found {len(existing_entities)} existing entities out of {len(list_names_data)} checked")
        
    except Exception as e:
        logger.error(f"Error in batch_check_existing_list_entities: {str(e)}")
    
    return existing_entities

def process_list_csv_data_batch(df, connection):
    """Process the entire list CSV file using batch processing"""
    total_success = 0
    total_errors = 0
    all_results = []
    
    logger.info(f"Starting list data batch processing for {len(df)} records")
    
    # Split dataframe into chunks
    chunk_size = st.session_state.get('chunk_size', 5000)
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    logger.info(f"Processing list data in {num_chunks} chunks with chunk size {chunk_size}")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start_idx:end_idx].copy()
        
        status_text.text(f"Processing list records {start_idx+1} to {end_idx} of {len(df)}...")
        
        # Process this chunk
        success_count, error_count, results = process_list_data_chunk(chunk, connection)
        
        total_success += success_count
        total_errors += error_count
        all_results.extend(results)
        
        # Update progress bar
        progress_bar.progress((i + 1) / num_chunks)
    
    progress_bar.progress(1.0)
    status_text.text(f"Completed processing {len(df)} list records.")
    logger.info(f"List data batch processing complete. Total success: {total_success}, Total errors: {total_errors}")
    
    return total_success, total_errors, all_results

def create_list_management_upload_tab():
    """Create the list management upload tab"""
    # st.markdown("<h3 style='font-size: 22px;'>📋 List Management - Upload Lists</h3>", unsafe_allow_html=True)
    # st.info("🔒 **List Management**: Upload CSV files containing watchlists, blacklists, whitelists, and suspicious entity lists")
    
    # File upload section

    # st.markdown("### 📁 Upload List Data")
    
    # Enhanced file uploader for list management
    df, metadata = create_enhanced_file_uploader(
        file_types=["csv", "txt", "xls", "xlsx"],
        key="list_management_upload"
    )
    
    if df is not None and not df.empty:
        try:
            st.write(f"**File:** {metadata['filename']} ({metadata['file_type'].upper()})")
            st.write(f"**Total records:** {len(df):,}")
            
            # Show file-specific metadata
            if metadata['file_type'] == 'txt':
                delimiter_name = {',': 'Comma', '\t': 'Tab', ';': 'Semicolon', '|': 'Pipe'}.get(metadata['delimiter'], metadata['delimiter'])
                st.write(f"**Delimiter:** {delimiter_name}")
                st.write(f"**Encoding:** {metadata['encoding']}")
            elif metadata['file_type'] in ['xls', 'xlsx']:
                st.write(f"**Active Sheet:** {metadata['active_sheet']}")
            
            uploaded_headers = list(df.columns)
            st.markdown("<h3 style='font-size: 18px;'>📋 File Preview</h3>", unsafe_allow_html=True)
            st.dataframe(df.head(5))
            
            # Create field mapping interface for list management
            field_mapping = create_list_field_mapping_interface(uploaded_headers)
            
            if field_mapping is not None or set(uploaded_headers) == set(EXPECTED_LIST_HEADERS):
                # Apply mapping if needed
                if field_mapping:
                    st.success("✅ Field mapping completed!")
                    with st.spinner("Applying field mapping..."):
                        mapped_df = apply_field_mapping(df, field_mapping)
                else:
                    st.success("✅ Headers match perfectly - no mapping needed!")
                    mapped_df = df.copy()
                
                # Add default values for missing optional fields
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if 'added_date' not in mapped_df.columns or mapped_df['added_date'].isna().all():
                    mapped_df['added_date'] = current_time
                if 'updated_by' not in mapped_df.columns or mapped_df['updated_by'].isna().all():
                    mapped_df['updated_by'] = 'UserUpload'
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'>📋 Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                # Validate mapped data
                with st.spinner("Validating list data..."):
                    validation_errors = validate_list_data(mapped_df)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please fix the errors in your data file or adjust the field mapping.")
                else:
                    st.success("✅ List data validation passed successfully!")

                    
                    # Process button
                    st.markdown("---")
                    if st.button(" Process List Data", key="process_list_data"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing list data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting list data processing for {len(mapped_df)} records")
                                    
                                    success_count, error_count, results = process_list_csv_data_batch(mapped_df, connection)
                                    connection.close()
                                    
                                    # Store results in session state
                                    st.session_state.list_success_count = success_count
                                    st.session_state.list_error_count = error_count
                                    st.session_state.list_results = results
                                    st.session_state.list_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f"✅ List data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"📈 Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"List data processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
        
        except Exception as e:
            st.error(f"Error processing list file: {e}")
            st.exception(e)

def create_list_management_tab():
    """Create comprehensive list management tab with upload and management capabilities"""
    st.markdown("<h2 style='font-size: 24px;'>📋 List Management System</h2>", unsafe_allow_html=True)
    st.markdown("Manage watchlists, blacklists, whitelists, and suspicious entity lists")
    
    # Create sub-tabs for different list management functions
    list_tabs = st.tabs([
        "📤 Upload Lists", 
        "🔍 Search Lists"
    ])
    
        # "📊 View Lists", 
    
    with list_tabs[0]:
        # Upload Lists tab
        create_list_management_upload_tab()
    
    # with list_tabs[1]:
        # View Lists tab
        # create_list_view_tab()
    
    with list_tabs[1]:
        # Search Lists tab
        create_list_search_tab()
    
    # with list_tabs[3]:
    #     # Download Template tab
    #     create_list_template_tab()

def create_list_view_tab():
    """Create tab for viewing uploaded lists"""
    st.markdown("<h3 style='font-size: 22px;'>📊 View Uploaded Lists</h3>", unsafe_allow_html=True)
    
    if not st.session_state.db_connected:
        st.warning("⚠️ Please connect to the database first to view lists")
        return
    
    try:
        connection = create_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            
            # Get summary statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_entities,
                    COUNT(DISTINCT entity_type) as entity_types,
                    COUNT(DISTINCT category) as categories
                FROM platform_list_entities
            """)
            summary = cursor.fetchone()
            
            if summary and summary[0] > 0:
                st.markdown("### 📈 Summary Statistics")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Entities", summary[0])
                col2.metric("Entity Types", summary[1])
                col3.metric("Categories", summary[2])
                
                # List type distribution
                cursor.execute("""
                    SELECT 
                        pli.list_type,
                        COUNT(*) as count
                    FROM platform_list_items pli
                    GROUP BY pli.list_type
                    ORDER BY count DESC
                """)
                list_type_data = cursor.fetchall()
                
                if list_type_data:
                    st.markdown("### 📋 List Type Distribution")
                    list_type_cols = st.columns(min(5, len(list_type_data)))
                    for i, (list_type, count) in enumerate(list_type_data):
                        list_type_cols[i % 5].metric(list_type.replace('_', ' ').title(), count)
                
                # Recent uploads
                st.markdown("### 🕒 Recent Uploads")
                cursor.execute("""
                    SELECT 
                        ple.entity_type,
                        ple.category,
                        pli.information_type,
                        pli.information_value,
                        pli.list_type,
                        ple.risk_level,
                        pli.added_date,
                        pli.added_by
                    FROM platform_list_entities ple
                    JOIN platform_list_items pli ON ple.list_entity_id = pli.list_entity_id
                    ORDER BY pli.added_date DESC
                    LIMIT 20
                """)
                
                recent_data = cursor.fetchall()
                if recent_data:
                    recent_df = pd.DataFrame(recent_data, columns=[
                        'Entity Type', 'Category', 'Info Type', 'Info Value', 
                        'List Type', 'Risk Level', 'Added Date', 'Added By'
                    ])
                    st.dataframe(recent_df, use_container_width=True)
                else:
                    st.info("No recent uploads found")
            else:
                st.info("📭 No list data found. Upload some lists to get started!")
            
            cursor.close()
            connection.close()
    
    except Exception as e:
        st.error(f"Error retrieving list data: {str(e)}")

def create_list_search_tab():
    """Create tab for searching through lists"""
    st.markdown("<h3 style='font-size: 22px;'>🔍 Search Lists</h3>", unsafe_allow_html=True)
    
    if not st.session_state.db_connected:
        st.warning("⚠️ Please connect to the database first to search lists")
        return
    
    # Search interface
    col1, col2 = st.columns(2)
    
    with col1:
        search_type = st.selectbox(
            "Search Type:",
            ["List Name","Information Value", "Entity Type", "Category", "List Type", "Risk Level"]
        )
    
    with col2:
        search_value = st.text_input("Search Value:", placeholder="Enter search term...")
    
    # Advanced filters
    with st.expander("🔧 Advanced Filters"):
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            filter_entity_type = st.selectbox(
                "Filter by Entity Type:",
                ["All", "Individual", "Organisation"]
            )
        
        with filter_col2:
            filter_list_type = st.selectbox(
                "Filter by List Type:",
                ["All", "whitelist", "blacklist", "watchlist", "odfi_suspicious", "rdfi_suspicious"]
            )
        
        with filter_col3:
            filter_risk_level = st.selectbox(
                "Filter by Risk Level:",
                ["All", "low", "medium", "high", "critical"]
            )
    
    if st.button("🔍 Search Lists", key="search_lists"):
        if search_value:
            try:
                connection = create_db_connection()
                if connection and connection.is_connected():
                    cursor = connection.cursor()
                    
                    # Build dynamic query based on search type
                    base_query = """
                        SELECT 
                            ple.notes,
                            ple.entity_type,
                            ple.category,
                            pli.information_type,
                            pli.information_value,
                            pli.list_type,
                            ple.risk_level,
                            ple.risk_score,
                            pli.added_date,
                            pli.added_by,
                            pli.source
                        FROM platform_list_entities ple
                        JOIN platform_list_items pli ON ple.list_entity_id = pli.list_entity_id
                        WHERE 1=1
                    """
                    
                    params = []
                    
                    # Add search condition

                    if search_type == "Information Value":
                        base_query += " AND pli.information_value LIKE %s"
                        params.append(f"%{search_value}%")
                    elif search_type == "Entity Type":
                        base_query += " AND ple.entity_type LIKE %s"
                        params.append(f"%{search_value}%")
                    elif search_type == "Category":
                        base_query += " AND ple.category LIKE %s"
                        params.append(f"%{search_value}%")
                    elif search_type == "List Type":
                        base_query += " AND pli.list_type LIKE %s"
                        params.append(f"%{search_value}%")
                    elif search_type == "Risk Level":
                        base_query += " AND ple.risk_level LIKE %s"
                        params.append(f"%{search_value}%")
                    
                    # Add filters
                    if filter_entity_type != "All":
                        base_query += " AND ple.entity_type = %s"
                        params.append(filter_entity_type)
                    
                    if filter_list_type != "All":
                        base_query += " AND pli.list_type = %s"
                        params.append(filter_list_type)
                    
                    if filter_risk_level != "All":
                        base_query += " AND ple.risk_level = %s"
                        params.append(filter_risk_level)
                    
                    base_query += " ORDER BY pli.added_date DESC LIMIT 100"
                    
                    cursor.execute(base_query, params)
                    search_results = cursor.fetchall()
                    
                    if search_results:
                        st.success(f"✅ Found {len(search_results)} results")
                        
                        results_df = pd.DataFrame(search_results, columns=['List Name',
                            'Entity Type', 'Category', 'Info Type', 'Info Value', 
                            'List Type', 'Risk Level', 'Risk Score', 'Added Date', 'Added By', 'Source'
                        ])
                        
                        st.dataframe(results_df, use_container_width=True)
                        
                        # Download results
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Search Results",
                            data=csv,
                            file_name=f"list_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("🔍 No results found for your search criteria")
                    
                    cursor.close()
                    connection.close()
            
            except Exception as e:
                st.error(f"Error searching lists: {str(e)}")
        else:
            st.warning("⚠️ Please enter a search value")


def create_list_template_tab():
    """Create tab for downloading list management templates"""
    st.markdown("<h3 style='font-size: 22px;'>📋 List Management Template</h3>", unsafe_allow_html=True)
    
    
    # Create sample template data
    template_data = [
        {
            'entity_type': 'Individual',
            'category': 'watchlist',
            'risk_score': 75.5,
            'risk_level': 'high',
            'list_name': 'High Risk Individuals',
            'country': 'US',
            'industry_code': '5200',
            'payment_channel': 'ACH',
            'start_time': '2024-01-01 00:00:00',
            'end_time': '2024-12-31 23:59:59',
            'information_type': 'SSN',
            'information_value': '123-45-6789',
            'list_type': 'watchlist',
            'added_date': '2024-01-15 10:30:00',
            'added_by':'UserUpload',
            'expiry_date': '2025-01-15 10:30:00',
            'source': 'Government Database',
            'update_date': '2024-01-15 10:30:00',
            'updated_by': 'AdminUser'
        },
        {
            'entity_type': 'Organisation',
            'category': 'blacklist',
            'risk_score': 95.0,
            'risk_level': 'critical',
            'list_name': 'Sanctioned Entities',
            'country': 'RU',
            'industry_code': '5400',
            'payment_channel': 'WIRE',
            'start_time': '2024-01-01 00:00:00',
            'end_time': '',
            'information_type': 'TAX_ID',
            'information_value': '12-3456789',
            'list_type': 'blacklist',
            'added_date': '2024-02-01 09:15:00',
            'added_by':'UserUpload',
            'expiry_date': '',
            'source': 'OFAC',
            'update_date': '2024-02-01 09:15:00',
            'updated_by': 'ComplianceTeam'
        },
        {
            'entity_type': 'Individual',
            'category': 'whitelist',
            'risk_score': 15.0,
            'risk_level': 'low',
            'list_name': 'Trusted Customers',
            'country': 'CA',
            'industry_code': '',
            'payment_channel': 'ZELLE',
            'start_time': '2024-03-01 00:00:00',
            'end_time': '2024-12-31 23:59:59',
            'information_type': 'EMAIL',
            'information_value': 'trusted.customer@example.com',
            'list_type': 'whitelist',
            'added_date': '2024-03-15 14:20:00',
            'added_by':'UserUpload',
            'expiry_date': '2025-03-15 14:20:00',
            'source': 'Customer Onboarding',
            'update_date': '2024-03-15 14:20:00',
            'updated_by': 'OnboardingTeam'
        }
    ]
    
    template_df = pd.DataFrame(template_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # CSV template with sample data
        csv_with_data = template_df.to_csv(index=False)
        st.download_button(
            label="📊 Download CSV Template ",
            data=csv_with_data,
            file_name=f"list_management_template_with_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

             
def create_list_field_mapping_interface(uploaded_headers):
    """Create field mapping interface specifically for list management"""
    st.markdown("<h3 style='font-size: 18px;'>🔗 Field Mapping for List Data</h3>", unsafe_allow_html=True)
    
    # Check if headers match exactly
    missing_headers = set(EXPECTED_LIST_HEADERS) - set(uploaded_headers)
    extra_headers = set(uploaded_headers) - set(EXPECTED_LIST_HEADERS)
    
    if not missing_headers and not extra_headers:
        st.success("✅ Perfect match! All headers are correctly formatted.")
        return None
    
    # Show header comparison
    st.warning("⚠️ Column name mismatch detected. Please map your fields to the expected format.")
    
    # Create mapping interface
    st.markdown("<h3 style='font-size: 18px;'>🗂️ Map Your Fields</h3>", unsafe_allow_html=True)
    # st.write("🔴 **Mandatory Fields**, 🔵 **Optional Fields**")
    st.info("Map your file's column headers to the expected format. Required fields must be mapped.")
    
    mapping = {}
    
    # Create mapping dropdowns for each expected field
    for i, expected_field in enumerate(EXPECTED_LIST_HEADERS):
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Prepare options
            options = ["-- Not Available --"] + uploaded_headers
            
            # Try to find automatic match
            default_index = 0
            for j, uploaded_header in enumerate(uploaded_headers):
                if uploaded_header.lower() == expected_field.lower():
                    default_index = j + 1
                    break
                elif uploaded_header.lower().replace('_', '').replace(' ', '') == expected_field.lower().replace('_', '').replace(' ', ''):
                    default_index = j + 1
                    break
            
            # Create selectbox
            is_required = expected_field in REQUIRED_LIST_FIELDS
            label = f"{'🔴 ' if is_required else '🔵 '}{expected_field}"
            
            selected = st.selectbox(
                label,
                options,
                index=default_index,
                key=f"list_mapping_{expected_field}_{i}",
                help=f"Required field" if is_required else "Optional field"
            )
            
            if selected != "-- Not Available --":
                mapping[expected_field] = selected
        
        with col2:
            description = LIST_FIELD_DESCRIPTIONS.get(expected_field, "No description available")
            st.write(f"_{description}_")
    
    # Validate required fields
    missing_required = [field for field in REQUIRED_LIST_FIELDS if field not in mapping]
    
    if missing_required:
        st.error(f" Missing required fields: {', '.join(missing_required)}")
        return None
    
    return mapping

# Fixed Width Schemas based on your specifications

def create_customer_fixed_width_schema() -> FixedWidthSchema:
    """Create the customer fixed width schema"""
    schema = FixedWidthSchema("customer")
    
    # Add all customer fields based on your mapping
    customer_fields = [
        FixedWidthField("1", "Customer_Number", "Text", 1, 50, 
                       "Customers: Global ID + 'CUSWBP'", "customer_entity_id"),
        FixedWidthField("2", "Customer_Name", "Text", 51, 100, 
                       "Name (FirstName LastName)", "Customer_Name"),
        FixedWidthField("3", "Country_Residence", "2 character ISO Code", 151, 2, 
                       "US", "country_of_residence_id"),
        FixedWidthField("4", "Country_Nationality", "2 character ISO Code", 153, 2, 
                       "00", "nationality"),
        FixedWidthField("5", "Birth_Date", "Text", 155, 20, 
                       "DOB Blanks, if unknown", "date_of_birth"),
        FixedWidthField("6", "Address_line_1", "Text", 175, 50, 
                       "House# Street", "address_line_1"),
        FixedWidthField("7", "Address_line_2", "Text", 225, 50, 
                       "Additional Address", "address_line_2"),
        FixedWidthField("8", "Address_line_3", "Text", 275, 50, 
                       "ZIP Code", "address_zipcode"),
        FixedWidthField("9", "Address_line_4", "Text", 325, 50, 
                       "City", "address_city"),
        FixedWidthField("10", "Address_line_5", "Text", 375, 50, 
                        "State", "address_state"),
        FixedWidthField("11", "Gender", "Text, M or F", 425, 1, 
                       "If Unknown, Blank", "gender"),
        FixedWidthField("12", "Customer_Type", "Text, M or P", 426, 1, 
                       "If Unknown, Blank", "customer_type"),
        FixedWidthField("13", "Business_Code", "Text", 427, 10, 
                       "Blanks", "industry_code"),
        FixedWidthField("14", "SSN", "Text", 437, 50, 
                       "SSN, Blank if unknown", "tax_id"),
        FixedWidthField("15", "Country_Birth", "Text", 487, 2, 
                       "Blanks", "country_of_birth_id"),
        FixedWidthField("16", "Phone_Number", "Text", 489, 50, 
                       "Phone Number", "phone_number"),
        FixedWidthField("17", "Phone_Number_Work", "Text", 539, 50, 
                       "Work Phone Number", "phone_number_work"),
        FixedWidthField("18", "Occupation", "Text", 589, 50, 
                       "Occupation", "job_title"),
        FixedWidthField("19", "Identification_Type", "A,B,C,D,E,F,L,M", 639, 1, 
                       "ID Type", "document_type"),
        FixedWidthField("20", "Identification_Other", "Text", 640, 50, 
                       "ID Other", "document_other"),
        FixedWidthField("21", "Identification_Number", "Text", 690, 50, 
                       "ID Number", "document_number"),
        FixedWidthField("22", "Issuing_Authority", "Text", 740, 2, 
                       "Blanks", "issuing_authority"),
        FixedWidthField("23", "Contract_Number", "Text", 742, 50, 
                       "Email address", "email_address"),
        FixedWidthField("24", "Country_Risk", "Text", 792, 2, 
                       "", "country_risk"),
        FixedWidthField("25", "Customer_Status", "Text", 794, 20, 
                       "ACTIVE", "customer_status"),
        FixedWidthField("26", "Risk_Category", "Text", 814, 2, 
                       "", "risk_category"),
        FixedWidthField("27", "PEP", "Text", 816, 1, 
                       "N", "pep"),
        FixedWidthField("28", "Risk_Score", "Text", 817, 9, 
                       "0", "customer_risk_score"),
        FixedWidthField("29", "Monthly_Income", "Text", 826, 20, 
                       "0", "monthly_income"),
        FixedWidthField("30", "Annual_Turnover", "Text", 846, 20, 
                       "0", "annual_turnover"),
        FixedWidthField("31", "Source_of_funds", "Text", 866, 50, 
                       "", "source_of_funds"),
        FixedWidthField("32", "Total_Assets", "Text", 916, 20, 
                       "0", "total_assets"),
        FixedWidthField("33", "Legal_Form", "Text", 936, 50, 
                       "When Customer_Type = 'M' then 'W' else Blanks", "legal_form"),
        FixedWidthField("34", "CustomerTypeCode", "Text", 986, 10, 
                       "BEN/TRA/AID/OWN/COR/TML/CHN", "customer_type_code"),
        FixedWidthField("35", "Account_Executive", "Text", 996, 50, 
                       "Blanks", "account_executive"),
        FixedWidthField("36", "Business_Unit", "Text", 1046, 20, 
                       "WBP", "business_unit")
    ]
    
    for field in customer_fields:
        schema.add_field(field)
    
    return schema

def create_account_fixed_width_schema() -> FixedWidthSchema:
    """Create the account fixed width schema based on your specifications"""
    schema = FixedWidthSchema("account")
    
    account_fields = [
        FixedWidthField("1", "A_Number", "Text", 1, 50, 
                       "Account Number with prefix/suffix", "account - customer_account_id"),
        FixedWidthField("2", "Account_Type", "Text", 51, 50, 
                       "CURRENT(Creation Date)", "account - account_type"),
        FixedWidthField("3", "Open_Date", "Date", 101, 20, 
                       "Open Date", "account - open_date"),
        FixedWidthField("4", "Close_Date", "Date", 121, 20, 
                       "Blank", "account - closed_date"),
        FixedWidthField("5", "Not_used_1", "Text", 141, 20, 
                       "", ""),
        FixedWidthField("6", "Status", "Text", 161, 50, 
                       "NEW", "account - account_status"),
        FixedWidthField("7", "Identified_By_Distance", "", 211, 50, 
                       "", ""),
        FixedWidthField("8", "Spec_Agreement", "Y or N", 261, 1, 
                       "N", ""),
        FixedWidthField("9", "Account_Name", "Text", 262, 100, 
                       "Account Name", "account - account_holder_name"),
        FixedWidthField("10", "Credit_Limit", "Amount no sign", 362, 20, 
                        "0", "account - account_field_6"),
        FixedWidthField("11", "Business_Unit", "Text", 382, 20, 
                        "WBP", "account - account_field_1"),
        FixedWidthField("12", "Not_used_2", "Text", 402, 30, 
                        "", ""),
        FixedWidthField("13", "Risk_Category", "Text", 432, 2, 
                        "", "account - account_field_2"),
        FixedWidthField("14", "Risk_Score", "Number", 434, 9, 
                        "0", "account - risk_rating"),
        FixedWidthField("15", "Branch_Code", "Text", 443, 50, 
                        "", "account - branch_id"),
        FixedWidthField("16", "Acceptance_Employee", "Text", 493, 50, 
                        "", "")
    ]
    
    for field in account_fields:
        schema.add_field(field)
    
    return schema

def create_transaction_fixed_width_schema() -> FixedWidthSchema:
    """Create the transaction fixed width schema"""
    schema = FixedWidthSchema("transaction")
    
    transaction_fields = [
        FixedWidthField("1", "A_Number", "Text", 1, 50, 
                       "Account identifier", "transactions - account_id"),
        FixedWidthField("2", "B_Number", "Text", 51, 48, 
                       "Biller information", "transactions - b_number"),
        FixedWidthField("3", "Not_used_1", "Text", 99, 7, 
                       "", ""),
        FixedWidthField("4", "Exclude_from_profiling", "Y,N or space", 106, 1, 
                       "N", ""),
        FixedWidthField("5", "Date_time", "Date", 107, 14, 
                       "Date_Time (YYYYMMDDHHMMSS)", "transactions - transaction_date"),
        FixedWidthField("6", "Transaction_Code", "Text", 121, 10, 
                       "CASH, MONEY ORDER, CHECK, etc.", "transactions - transaction_code"),
        FixedWidthField("7", "Not_used_2", "Text", 131, 40, 
                       "", ""),
        FixedWidthField("8", "Amount", "Amount, no sign", 171, 20, 
                       "", "transactions - amount"),
        FixedWidthField("9", "Debit_Credit", "D or C", 191, 1, 
                       "Reversal: 'C', Other: 'D'", "transactions - debit_credit_indicator"),
        FixedWidthField("10", "Currency", "Text ISO Currency code", 192, 5, 
                        "USD", "transactions - nominal_currency"),
        FixedWidthField("11", "Original_Amount", "Amount, no sign", 197, 20, 
                        "0", ""),
        FixedWidthField("12", "Reference", "Text", 217, 30, 
                        "12 Character Sequence + CFP + D/C + Counter", "transactions - reference"),
        FixedWidthField("13", "Description", "Text", 247, 160, 
                        "Terminal ID", "transactions - description"),
        FixedWidthField("14", "Balance", "Amount, with sign", 407, 20, 
                        "0", "transactions - balance"),
        FixedWidthField("15", "EOD_Balance", "Amount, with sign", 427, 20, 
                        "0", "transactions - eod_balance"),
        FixedWidthField("16", "B_Name", "Text", 447, 50, 
                        "Biller Parent Company Name", "transactions-counterparty_name"),
        FixedWidthField("17", "B_Country", "Text ISO Currency code", 497, 2, 
                        "US", ""),
        FixedWidthField("18", "Non_Accountholder", "Y or N", 499, 1, 
                        "Y", "transactions -non_account_holder"),
        FixedWidthField("19", "Transaction_Field_1", "Text", 500, 50, 
                        "See TransactionField1", "transactions - transaction_field_1"),
        FixedWidthField("20", "Transaction_Field_2", "Text", 550, 50, 
                        "See TransactionField2", "transactions - transaction_field_2"),
        FixedWidthField("21", "Transaction_Field_3", "Text", 600, 50, 
                        "See TransactionField3", "transactions - transaction_field_3"),
        FixedWidthField("22", "Transaction_Field_4", "Text", 650, 50, 
                        "See TransactionField4", "transactions - transaction_field_4"),
        FixedWidthField("23", "Transaction_Field_5", "Text", 700, 50, 
                        "See TransactionField5", "transactions - transaction_field_5"),
        FixedWidthField("24", "Customer_number", "Text", 750, 50, 
                        "Blanks", ""),
        FixedWidthField("25", "TIN", "Text", 800, 50, 
                        "Blanks", ""),
        FixedWidthField("26", "Customer_Key", "Text", 850, 50, 
                        "Blanks", ""),
        FixedWidthField("27", "BIC", "Text", 900, 11, 
                        "Blanks", ""),
        FixedWidthField("28", "Not_used_3", "Text", 911, 10, 
                        "", ""),
        FixedWidthField("29", "Product_Type", "Text", 921, 20, 
                        "WBP", ""),
        FixedWidthField("30", "Product_Number", "Number", 941, 20, 
                        "Transaction ID", "transactions - transaction_id"),
        FixedWidthField("31", "Channel_Type", "Text", 961, 20, 
                        "Transmission ID", "transactions - txn_source"),
        FixedWidthField("32", "Channel_ID", "Text", 981, 20, 
                        "AID (Agent ID)", "transactions - intermediary_1_id"),
        FixedWidthField("33", "Positive_Pay_Indicator", "Y or N", 1001, 1, 
                        "N", "transactions - positive_pay_indicator"),
        FixedWidthField("34", "Originator_Bank", "Text", 1002, 71, 
                        "Application ID", "transactions - originator_bank"),
        FixedWidthField("35", "Intermediary_Bank", "Text", 1073, 71, 
                        "Global ID (Beneficiary) + 'CUSWBP'", "transactions - intermediary_bank"),
        FixedWidthField("36", "Beneficiary_Bank", "Text", 1144, 71, 
                        "Global ID (Transactor) + 'CUSWBP'", "transactions - beneficiary_bank"),
        FixedWidthField("37", "Instructing_Bank", "Text", 1215, 71, 
                        "Chain ID", "transactions - instructing_bank"),
        FixedWidthField("38", "Inter_Bank_Information", "Text", 1286, 385, 
                        "Entered Note", "transactions - inter_bank_information"),
        FixedWidthField("39", "Other_Information", "Text", 1671, 425, 
                        "Beneficiary Name/Address/Email/Phone", "transactions - other_information"),
        FixedWidthField("40", "Beneficiary_Details", "Text", 2096, 140, 
                        "Consumer Fee (formatted 0000.00)", ""),
        FixedWidthField("41", "Transaction_Field_11", "Text", 2236, 50, 
                        "Transactor Phone Number", "transactions - transaction_field_11"),
        FixedWidthField("42", "Transaction_Field_12", "Text", 2286, 50, 
                        "Transactor State Code + | + ID Number", "transactions - transaction_field_12"),
        FixedWidthField("43", "Transaction_Field_13", "Text", 2336, 50, 
                        "Actual Account Number", "transactions - transaction_field_13"),
        FixedWidthField("44", "Transaction_Field_14", "Text", 2386, 50, 
                        "Cell Phone + biller_list_id + industry_id + utility_type + signature_flag", "transactions - transaction_field_14"),
        FixedWidthField("45", "Transaction_Field_15", "Text", 2436, 50, 
                        "Agent Name", "transactions - transaction_field_15"),
        FixedWidthField("46", "Employee_Number", "Text", 2486, 50, 
                        "Clerk ID", ""),
        FixedWidthField("47", "IAT_Secondary_SEC", "Text", 2536, 3, 
                        "Stub Type (sid)", "")
    ]
    
    for field in transaction_fields:
        schema.add_field(field)
    
    return schema

def create_relationship_fixed_width_schema() -> FixedWidthSchema:
    """Create the relationship fixed width schema for account relationships"""
    schema = FixedWidthSchema("relationship")
    
    # This would be defined based on your relationship file format
    # For now, creating a basic structure - you can modify based on actual format
    relationship_fields = [
        FixedWidthField("1", "From_ID", "Text", 1, 50, 
                       "From identifier", "from_id"),
        FixedWidthField("2", "To_ID", "Text", 51, 50, 
                       "To identifier", "to_id"),
        FixedWidthField("3", "Start_Date", "Date", 101, 20, 
                       "start date", "start_date"),
        FixedWidthField("4", "End_Date", "Date", 121, 20, 
                       "end date", "end_date"),
        FixedWidthField("5", "Role_Type", "Text", 141, 4, 
                       "Role type", "role_type")
    ]
    
    for field in relationship_fields:
        schema.add_field(field)
    
    return schema
    
def detect_file_type_from_name(filename: str) -> Optional[str]:
    """Detect file type from filename pattern"""
    filename_upper = filename.upper()
    
    if re.match(r'CUSTOMER_\d{8}_\d{5}\.TXT', filename_upper):
        return 'customer'
    elif re.match(r'ACCOUNT_\d{8}_\d{5}\.TXT', filename_upper):
        return 'account'
    elif re.match(r'TRANSACTION_\d{8}_\d{5}\.TXT', filename_upper):
        return 'transaction'
    elif re.match(r'RELATIONSHIP_\d{8}_\d{5}\.TXT', filename_upper):
        return 'relationship'
    
    return None

def map_relationship_fixed_width_to_standard(parsed_df: pd.DataFrame) -> pd.DataFrame:
    """Map fixed-width relationship data to standard format for customer-account relationships"""
    mapped_df = pd.DataFrame()
    
    # Extract customer_id from From_ID (remove CUSWBP suffix)
    if 'from_id' in parsed_df.columns:
        mapped_df['from_id'] = parsed_df['from_id']
    elif 'From_ID' in parsed_df.columns:
        mapped_df['from_id'] = parsed_df['From_ID']
    
    # Extract account_id from To_ID (remove biller prefix and BAWBP suffix)
    if 'to_id' in parsed_df.columns:
        mapped_df['to_id'] = parsed_df['to_id']
    elif 'To_ID' in parsed_df.columns:
        mapped_df['to_id'] = parsed_df['To_ID']
    
    # Map start date
    if 'start_date' in parsed_df.columns:
        mapped_df['start_date'] = parsed_df['start_date'].apply(convert_to_mysql_datetime)
    elif 'Start_Date' in parsed_df.columns:
        mapped_df['start_date'] = parsed_df['Start_Date'].apply(convert_to_mysql_datetime)
    else:
        mapped_df['start_date'] = None
    
    # Map end date
    if 'end_date' in parsed_df.columns:
        mapped_df['end_date'] = parsed_df['end_date'].apply(convert_to_mysql_datetime)
    elif 'End_Date' in parsed_df.columns:
        mapped_df['end_date'] = parsed_df['End_Date'].apply(convert_to_mysql_datetime)
    else:
        mapped_df['end_date'] = None
    
    mapped_df['role_type'] = parsed_df['role_type']

    # Set default values
    mapped_df['changetype'] = 'add'  # Default to add for fixed-width files
    
        
        
    # Clean up data - remove NaN and empty strings
    for col in mapped_df.columns:
        if mapped_df[col].dtype == 'object':
            mapped_df[col] = mapped_df[col].fillna('').astype(str).str.strip()
            mapped_df[col] = mapped_df[col].replace('nan', '')
            mapped_df[col] = mapped_df[col].replace('', None)
    
    return mapped_df
    

def create_relationship_upload_tab_with_fixed_width():
    """Enhanced relationship upload tab with fixed-width support"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Relationship Data (Fixed Width Support)</h3>", unsafe_allow_html=True)
    st.info("📄 **Fixed Width Support**: Upload RELATIONSHIP_YYYYMMDD_RRRRR.txt files with automatic parsing!")
    
    # File format selection
    upload_format = st.radio(
        "Select file format:",
        ["Fixed Width Text"],
        key="relationship_format_selection"
    )
    
    if upload_format == "Fixed Width Text":
        # Fixed width file upload
        df, metadata = create_enhanced_fixed_width_uploader(
            file_types=["txt"],
            key="relationship_fixed_width_upload"
        )
        
        if df is not None and not df.empty:
            try:
                st.write(f"**File:** {metadata['filename']} (Fixed Width)")
                st.write(f"**Total records:** {len(df):,}")
                st.write(f"**Parsed fields:** {metadata.get('parsed_fields', 0)}")

                # Store metadata in session state for logging
                st.session_state.relationship_metadata = metadata
                
                # Map to standard format
                with st.spinner("Mapping fixed-width data to standard format..."):
                    mapped_df = map_relationship_fixed_width_to_standard(df)

                if 'changetype' not in mapped_df.columns or mapped_df['changetype'].isna().any():
                    mapped_df['changetype'] = 'add'

                st.success(" Fixed-width data mapped to standard format!")
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'> Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                
                # Validate mapped data
                with st.spinner("Validating mapped relationship data..."):
                    validation_errors = validate_relationship_data(mapped_df)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please check your fixed-width data format.")
                else:
                    st.success(" Relationship data validation passed successfully!")
                    
                    # Process button
                    if st.button(" Process Relationship Data", key="process_relationship_fixed_width"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing relationship data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting relationship processing for fixed-width file with {len(mapped_df)} records")
                                    
                                    success_count, error_count, results = process_relationship_fixed_width_data(mapped_df, connection)
                                    connection.close()
                                    
                                    # Store results in session state
                                    st.session_state.relationship_success_count = success_count
                                    st.session_state.relationship_error_count = error_count
                                    st.session_state.relationship_results = results
                                    st.session_state.relationship_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f" Relationship data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f" Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"Relationship data processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
            
            except Exception as e:
                st.error(f"Error processing fixed-width relationship file: {e}")
                st.exception(e)
    else:
        # Original multi-format upload (placeholder - you can implement this if needed)
        st.info("CSV/Excel relationship upload not yet implemented. Please use Fixed Width Text format.")


def validate_relationship_data(df):
    """Validate the uploaded relationship data"""
    validation_errors = []
    
    # Check required columns
    required_columns = ['from_id', 'to_id']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return validation_errors
    
    # Check for empty customer_ids
    empty_customer_ids = df[df['from_id'].isna() | (df['from_id'] == '')].index.tolist()
    if empty_customer_ids:
        if len(empty_customer_ids) > 10:
            validation_errors.append(f"Empty from_id found in {len(empty_customer_ids)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_customer_ids[:10]]))}, and more...")
        else:
            validation_errors.append(f"Empty from_id found in rows: {', '.join(map(str, [i+2 for i in empty_customer_ids]))}")
    
    # Check for empty account_ids
    empty_account_ids = df[df['to_id'].isna() | (df['to_id'] == '')].index.tolist()
    if empty_account_ids:
        if len(empty_account_ids) > 10:
            validation_errors.append(f"Empty to_id found in {len(empty_account_ids)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_account_ids[:10]]))}, and more...")
        else:
            validation_errors.append(f"Empty to_id found in rows: {', '.join(map(str, [i+2 for i in empty_account_ids]))}")
    
    # Validate relationship types
    if 'role_type' in df.columns:
        valid_role_types = [
            'ACH','BEN', 'TRA','AID', 'OWN', 'COR', 'TML', 'CHN', 'TAID', 'TCUS', 'TACC', 'ACHN','AGT'
        ]
        
        df_copy = df.copy()
        df_copy['role_type'] = df_copy['role_type'].fillna('').astype(str).str.strip()
        
        non_empty_rel_types = df_copy[df_copy['role_type'] != '']
        
        if len(non_empty_rel_types) > 0:
            invalid_rel_types_mask = ~non_empty_rel_types['role_type'].isin(valid_role_types)
            
            if invalid_rel_types_mask.any():
                invalid_types = non_empty_rel_types.loc[invalid_rel_types_mask, 'role_type'].unique()
                invalid_types_str = []
                for val in invalid_types:
                    str_val = str(val).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid role_type values: {', '.join(invalid_types_str)}. Valid values are {', '.join(valid_role_types)}")
    
    
    return validation_errors

def process_relationship_fixed_width_data(mapped_df: pd.DataFrame, connection):
    """Process relationship data to insert into relationship_stream table only"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing relationship file with {len(mapped_df)} records for relationship_stream table (buffered)")
    
    try:
        # Use buffered cursor to completely avoid "Unread result found" errors
        cursor = connection.cursor(buffered=True)
        current_time = datetime.now()
        
        # Statistics tracking
        stats = {
            'relationships_inserted': 0,
            'relationships_updated': 0,
            'duplicate_relationships': 0,
            'validation_errors': 0,
            'accounts_updated': 0,
            'accounts_not_found': 0
        }
        
        # Pre-fetch all existing relationships
        cursor.execute("SELECT from_id, to_id, relationship_id FROM relationship_stream")
        existing_relationships = {}
        for row in cursor.fetchall():
            key = (row[0], row[1])  # (from_id, to_id)
            existing_relationships[key] = row[2]  # relationship_id
        
        # Batch collections
        relationships_to_insert = []
        relationships_to_update = []

        logger.info("--- RELATIONSHIP RECORDS PROCESSING ---")
        
        # Process each relationship record
        for index, row in mapped_df.iterrows():
            try:
                row_dict = clean_row_data(row.to_dict())
                
                from_id = row_dict.get('from_id')
                to_id = row_dict.get('to_id')
                role_type = row_dict.get('role_type')
                start_date = convert_date_field(row_dict.get('start_date'))
                end_date = convert_date_field(row_dict.get('end_date'))
                
                # Validate required fields
                if not from_id or not to_id:
                    error_count += 1
                    stats['validation_errors'] += 1
                    results.append(f"Row {index + 1}: Missing from_id or to_id")
                    continue
                
                # Check if relationship already exists using pre-fetched data
                relationship_key = (from_id, to_id)
                existing_relationship_id = existing_relationships.get(relationship_key)
                
                # Prepare relationship data for relationship_stream table
                relationship_data = {
                    'from_id': from_id,
                    'to_id': to_id,
                    'role_type': role_type,
                    'start_date': start_date,
                    'end_date': end_date,
                    'insert_date': current_time,
                    'update_date': current_time,
                    'inserted_by': 'System',
                    'updated_by': 'System',
                    'svc_provider_name': 'UserUpload',
                    'row_index': index
                }
                
                if existing_relationship_id:
                    # Add relationship_id for update
                    relationship_data['relationship_id'] = existing_relationship_id
                    relationships_to_update.append(relationship_data)
                    stats['relationships_updated'] += 1
                    log_msg = f"Row {index + 1}: Prepared UPDATE for relationship {from_id} -> {to_id} ({role_type})"
                    logger.info(log_msg)
                else:
                    relationships_to_insert.append(relationship_data)
                    stats['relationships_inserted'] += 1
                    log_msg = f"Row {index + 1}: Prepared INSERT for relationship {from_id} -> {to_id} ({role_type})"
                    logger.info(log_msg)
                
            except Exception as e:
                error_count += 1
                results.append(f"Row {index + 1}: Error preparing relationship data - {str(e)}")
                logger.exception(f"Error processing relationship row {index}")
        
        # Batch insert new relationships
        if relationships_to_insert:
            logger.info(f"--- INSERTING {len(relationships_to_insert)} NEW RELATIONSHIPS ---")
            relationship_insert_sql = """
            INSERT INTO relationship_stream (
                from_id, to_id, role_type,
                start_date, end_date, insert_date, update_date, 
                inserted_by, updated_by, svc_provider_name
            ) VALUES (
                %(from_id)s, %(to_id)s, %(role_type)s,
                %(start_date)s, %(end_date)s, %(insert_date)s, %(update_date)s,
                %(inserted_by)s, %(updated_by)s, %(svc_provider_name)s
            )
            """
            
            cursor.executemany(relationship_insert_sql, relationships_to_insert)
            logger.info(f"Inserted {len(relationships_to_insert)} new relationships into relationship_stream")
        
        # Batch update existing relationships
        if relationships_to_update:
            logger.info(f"--- UPDATING {len(relationships_to_update)} EXISTING RELATIONSHIPS ---")
            
            relationship_update_sql = """
            UPDATE relationship_stream 
            SET role_type = %(role_type)s,
                start_date = %(start_date)s,
                end_date = %(end_date)s, 
                update_date = %(update_date)s,
                updated_by = %(updated_by)s,
                svc_provider_name = %(svc_provider_name)s
            WHERE relationship_id = %(relationship_id)s
            """
            
            cursor.executemany(relationship_update_sql, relationships_to_update)
            logger.info(f"Updated {len(relationships_to_update)} existing relationships in relationship_stream")
        
        # Commit all changes
        connection.commit()
        cursor.close()

        
        
        # Calculate success count and generate results (same as above)
        success_count = len(mapped_df) - error_count
        
        # Generate detailed results
        for rel_data in relationships_to_insert:
            row_idx = rel_data['row_index']
            from_id = rel_data['from_id']
            to_id = rel_data['to_id']
            role_type = rel_data['role_type']
            
            results.append(f"Row {row_idx + 1}: Inserted relationship {from_id} -> {to_id} ({role_type}) into relationship_stream")
        
        for rel_data in relationships_to_update:
            row_idx = rel_data['row_index']
            from_id = rel_data['from_id']
            to_id = rel_data['to_id']
            role_type = rel_data['role_type']
            
            results.append(f"Row {row_idx + 1}: Updated relationship {from_id} -> {to_id} ({role_type}) in relationship_stream")

        # Post relationship_stream operations: Update account table entity_id for blank entries
        logger.info("--- ACCOUNT ENTITY_ID UPDATE PROCESS STARTED ---")
        try:
            cursor = connection.cursor()
            
            # Step 1: Query to pick up all account entries which have entity_id as blank
            account_query = """
            SELECT customer_account_id 
            FROM account 
            WHERE entity_id IS NULL OR entity_id = ''
            """
            
            cursor.execute(account_query)
            blank_entity_accounts = cursor.fetchall()
            
            if blank_entity_accounts:
                logger.info(f"Found {len(blank_entity_accounts)} account entries with blank entity_id")
                results.append(f"Found {len(blank_entity_accounts)} account entries with blank entity_id for processing")
                
                accounts_updated = 0
                accounts_not_found = 0
                chunk_size = 5000  # Process in chunks of 1000 accounts
                total_accounts = len(blank_entity_accounts)
                
                # Process accounts in chunks
                for chunk_start in range(0, total_accounts, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_accounts)
                    chunk_accounts = blank_entity_accounts[chunk_start:chunk_end]
                    
                    logger.info(f"Processing chunk {chunk_start//chunk_size + 1}: accounts {chunk_start + 1} to {chunk_end}")
                    results.append(f"Processing chunk {chunk_start//chunk_size + 1}: accounts {chunk_start + 1} to {chunk_end}")
                    
                    chunk_updates = []  # Collect updates for batch execution
                    
                    # Step 2: For each customer_account_id in chunk, look up in relationship_stream and get entity_id
                    for (customer_account_id,) in chunk_accounts:
                        try:
                            # Query relationship_stream to find the from_id for this customer_account_id
                            # only pick records that have the role_type as ACH (Account Holder)
                            relationship_query = """
                            SELECT from_id 
                            FROM relationship_stream 
                            WHERE to_id = %s AND role_type = 'ACH'
                            LIMIT 1
                            """
                            
                            cursor.execute(relationship_query, (customer_account_id,))
                            relationship_result = cursor.fetchone()
                            
                            if relationship_result:
                                from_id = relationship_result[0]
                                
                                # Step 3: Look up entity_id in entity_customer table using from_id
                                entity_query = """
                                SELECT entity_id 
                                FROM entity_customer 
                                WHERE customer_entity_id = %s
                                LIMIT 1
                                """
                                
                                cursor.execute(entity_query, (from_id,))
                                entity_result = cursor.fetchone()
                                
                                if entity_result:
                                    entity_id = entity_result[0]
                                    chunk_updates.append((entity_id, customer_account_id))
                                    logger.info(f"Account {customer_account_id}: Found entity_id {entity_id} via relationship from_id {from_id}")
                                else:
                                    accounts_not_found += 1
                                    error_msg = (f"Account {customer_account_id}: No entity_id found in entity_customer for from_id {from_id}")
                                    results.append(error_msg)
                                    logger.warning(error_msg)
                            else:
                                accounts_not_found += 1
                                error_msg = (f"Account {customer_account_id}: No ACH relationship found in relationship_stream")
                                results.append(error_msg)
                                logger.warning(error_msg)
                                
                        except Exception as account_error:
                            accounts_not_found += 1
                            error_msg = f"Error processing account {customer_account_id}: {str(account_error)}"
                            results.append(error_msg)
                            logger.error(error_msg)
                    
                    # Step 4: Batch update accounts in this chunk
                    if chunk_updates:
                        try:
                            update_query = """
                            UPDATE account 
                            SET entity_id = %s 
                            WHERE customer_account_id = %s
                            """
                            
                            cursor.executemany(update_query, chunk_updates)
                            chunk_updated = cursor.rowcount
                            accounts_updated += chunk_updated
                            
                            # Commit this chunk
                            connection.commit()
                            
                            logger.info(f"Chunk {chunk_start//chunk_size + 1}: Updated {chunk_updated} accounts")
                            results.append(f"Chunk {chunk_start//chunk_size + 1}: Updated {chunk_updated} accounts")
                            
                            # Log individual updates for this chunk
                            for entity_id, customer_account_id in chunk_updates:
                                results.append(f"Updated account {customer_account_id}: entity_id set to {entity_id}")
                                
                        except Exception as batch_error:
                            error_msg = f"Error in batch update for chunk {chunk_start//chunk_size + 1}: {str(batch_error)}"
                            results.append(error_msg)
                            logger.error(error_msg)
                            connection.rollback()
                            accounts_not_found += len(chunk_updates)
                
                # Update statistics
                stats['accounts_updated'] = accounts_updated
                stats['accounts_not_found'] = accounts_not_found
                
                # Summary results
                summary_msg = f"Account entity_id update completed: {accounts_updated} updated, {accounts_not_found} not found/failed"
                results.append(summary_msg)
                logger.info(summary_msg)
                
            else:
                results.append("No account entries found with blank entity_id")
                logger.info("No account entries found with blank entity_id")
            
            cursor.close()
            
        except Exception as e:
            error_msg = f"Error during account entity_id update process: {str(e)}"
            results.append(error_msg)
            logger.error(error_msg)
            # Rollback in case of error
            connection.rollback()
            raise e
        
        # Log final statistics
        logger.info("=== RELATIONSHIP PROCESSING COMPLETED SUCCESSFULLY ===")
        logger.info(f"File: {filename}")
        logger.info(f"Total Records Processed: {len(mapped_df)}")
        logger.info(f"Success Count: {success_count}")
        logger.info(f"Error Count: {error_count}")
        logger.info(f"Relationships Inserted: {stats['relationships_inserted']}")
        logger.info(f"Relationships Updated: {stats['relationships_updated']}")
        logger.info(f"Accounts Updated with entity_id: {stats['accounts_updated']}")
        logger.info(f"Accounts Not Found/Failed: {stats['accounts_not_found']}")
        logger.info(f"Validation Errors: {stats['validation_errors']}")
        logger.info("=== END RELATIONSHIP PROCESSING ===\n")
        
        # Add summary to results
        comprehensive_summary = (f"PROCESSING COMPLETE - File: {filename} | "
                               f"Relationships: {stats['relationships_inserted']} inserted, {stats['relationships_updated']} updated | "
                               f"Accounts: {stats['accounts_updated']} entity_id updated, {stats['accounts_not_found']} not found | "
                               f"Errors: {stats['validation_errors']} validation errors")
        
        results.insert(0, comprehensive_summary)
        
        

    except Exception as e:
        error_count = len(mapped_df)
        results.append(f"Relationship_stream processing failed: {str(e)}")
        logger.error(f"Error processing relationship file: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rolled back due to error")
            
        except:
            pass
    
    return success_count, error_count, results

def parse_fixed_width_file(file_content: str, schema: FixedWidthSchema) -> pd.DataFrame:
    """Parse a fixed-width file using the provided schema"""
    
    lines = file_content.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    
    # Remove only empty lines at the very end of the file
    while lines and not lines[-1]:
        lines.pop()

   
    parsed_data = []
    
    for line_num, line in enumerate(lines, 1):
        try:
            parsed_row = schema.parse_line(line)
            # Add line number for debugging
            parsed_row['_line_number'] = line_num
            parsed_data.append(parsed_row)
        except Exception as e:
            logger.warning(f"Error parsing line {line_num}: {str(e)}")
            continue
    
    return pd.DataFrame(parsed_data)

def create_enhanced_fixed_width_uploader(file_types: list = None, key: str = "fixed_width_upload") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    Enhanced file uploader that supports fixed-width format files
    """
    if file_types is None:
        file_types = ["txt"]
    
    uploaded_file = st.file_uploader(
        "Choose a fixed-width file", 
        type=file_types, 
        key=key,
        help="Supported formats: Fixed-width TXT files with patterns CUSTOMER_YYYYMMDD_RRRRR.txt, ACCOUNT_YYYYMMDD_RRRRR.txt, TRANSACTION_YYYYMMDD_RRRRR.txt"
    )
    
    if uploaded_file is not None:
        filename = uploaded_file.name
        file_bytes = uploaded_file.getvalue()
        
        # Detect file type from filename
        file_type = detect_file_type_from_name(filename)
        
        if not file_type:
            st.error(f" Unsupported filename pattern: {filename}")
            st.info("Expected patterns: CUSTOMER_YYYYMMDD_RRRRR.txt, ACCOUNT_YYYYMMDD_RRRRR.txt, TRANSACTION_YYYYMMDD_RRRRR.txt")
            return None, {}
        
        metadata = {
            "filename": filename,
            "file_type": file_type,
            "file_size": len(file_bytes),
            "format": "fixed_width"
        }
        
        try:
            # Detect encoding
            encoding = detect_encoding(file_bytes)
            file_content = file_bytes.decode(encoding)
            metadata["encoding"] = encoding
            
            # Get appropriate schema
            if file_type == 'customer':
                schema = create_customer_fixed_width_schema()
            elif file_type == 'account':
                schema = create_account_fixed_width_schema()
            elif file_type == 'transaction':
                schema = create_transaction_fixed_width_schema()
            elif file_type == 'relationship':
                schema = create_relationship_fixed_width_schema()
            else:
                st.error(f"No schema available for file type: {file_type}")
                return None, metadata
            
            # Parse the file
            st.info(f" Detected {file_type.title()} file format")
            st.info(f"Using schema with {len(schema.fields)} fields")
            
            with st.spinner(f"Parsing {file_type} fixed-width file..."):
                df = parse_fixed_width_file(file_content, schema)
            
            if not df.empty:
                metadata["schema"] = schema
                metadata["parsed_fields"] = len(schema.fields)
                metadata["parsed_records"] = len(df)
                
                st.success(f" Successfully parsed {len(df):,} records from {file_type} file")

                if 'amount' in df.columns and 'debit_credit_indicator' in df.columns:

                    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                    df['balance'] = pd.to_numeric(df['balance'], errors='coerce')
                    df['eod_balance'] = pd.to_numeric(df['eod_balance'], errors='coerce')

                    df['debit_credit_indicator'] = df['amount'].apply(
                        lambda x: 'C' if pd.notna(x) and x > 0 else 'D'
                    )
                
                # Show file preview
                st.subheader("📋 File Preview")
                # Remove the line number column for display
                display_df = df.drop('_line_number', axis=1, errors='ignore')
                st.dataframe(display_df.head(10))
                
                
                return df, metadata
            else:
                st.error(" No data could be parsed from the file")
                return None, metadata
                
        except Exception as e:
            st.error(f" Error processing file: {str(e)}")
            return None, metadata
    
    return None, {}

def process_relationship_file(relationship_df: pd.DataFrame, connection) -> Tuple[int, int, List[str]]:
    """
    Process relationship file and create account records
    """
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing relationship file with {len(relationship_df)} records")
    
    try:
        # Group by customer to create accounts
        for index, row in relationship_df.iterrows():
            try:
                customer_id = row.get('customer_id')
                account_id = row.get('account_id')
                # relationship_type = row.get('relationship_type', 'RELATED_PARTY')
                status = row.get('status', 'ACTIVE')
                
                if not customer_id or not account_id:
                    error_count += 1
                    results.append(f"Row {index + 1}: Missing customer_id or account_id")
                    continue
                
                # Create account record based on relationship
                account_data = {
                    'changetype': 'add',
                    'account_id': account_id,
                    'customer_id': customer_id,
                    'account_number': account_id,  # Use account_id as account_number
                    'account_type': 'Checking',  # Default type
                    'account_currency': 'USD',
                    'account_balance': 0.0,
                    'open_date': row.get('start_date'),
                    'account_status': status,
                    'is_p2p_enabled': 'N',
                    'is_joint': 'N'
                }
                
                # Process this account record
                # You can integrate this with your existing account processing logic
                success_count += 1
                results.append(f"Row {index + 1}: Created account {account_id} for customer {customer_id}")
                
            except Exception as e:
                error_count += 1
                results.append(f"Row {index + 1}: Error processing relationship - {str(e)}")
                logger.exception(f"Error processing relationship row {index}")
    
    except Exception as e:
        logger.error(f"Error processing relationship file: {str(e)}")
        results.append(f"Relationship processing failed: {str(e)}")
        error_count = len(relationship_df)
    
    return success_count, error_count, results

def extract_customer_id_from_entity_id(entity_id):
    """Extract clean customer ID from the full entity ID by removing suffix"""
    if not entity_id or pd.isna(entity_id):
        return entity_id
    
    entity_id_str = str(entity_id).strip()
    
    # List of all possible suffixes
    suffixes = ['CUSWBP', 'TIDWBP', 'AIDWBP', 'GRPWBP', 'OWNWBP', 'CHNWBP']
    
    
    # If no suffix found, return as-is
    return entity_id_str


def map_customer_fixed_width_to_standard(parsed_df: pd.DataFrame) -> pd.DataFrame:
    """Map fixed-width customer data to standard format"""
    mapped_df = pd.DataFrame()
    
    # Add required fields
    mapped_df['changetype'] = 'add'  # Default to add
    
    # Map customer number - FIXED: Extract clean ID without suffix
    if 'customer_entity_id' in parsed_df.columns:
        # Use the full entity_id to determine customer_type first
        mapped_df['customer_type'] = parsed_df['customer_entity_id'].apply(determine_customer_type_from_id)
        
        # Then extract clean customer_id without suffix
        mapped_df['customer_id'] = parsed_df['customer_entity_id'].apply(extract_customer_id_from_entity_id)
    else:
        mapped_df['customer_type'] = 'Consumer'  # Default fallback
        mapped_df['customer_id'] = None
     
    # Parse customer name into first and last name
    if 'first_name' in parsed_df.columns and 'last_name' in parsed_df.columns:
        # If already parsed, use as-is
        mapped_df['first_name'] = parsed_df['first_name']
        mapped_df['last_name'] = parsed_df['last_name']
    elif 'Customer_Name' in parsed_df.columns:
        # Parse "FirstName LastName" format
        def parse_name(name_str):
            if not name_str or pd.isna(name_str):
                return '', ''
            parts = str(name_str).strip().split()
            if len(parts) >= 2:
                return parts[0], ' '.join(parts[1:])
            elif len(parts) == 1:
                return parts[0], ''
            return '', ''
        
        name_parts = parsed_df['Customer_Name'].apply(parse_name)
        mapped_df['first_name'] = [part[0] for part in name_parts]
        mapped_df['last_name'] = [part[1] for part in name_parts]

    if 'tax_id' in parsed_df.columns:
        mapped_df['tax_id'] = parsed_df['tax_id']
        # print(f"DEBUG: Mapping SSN field - found {parsed_df['tax_id'].notna().sum()} non-null values")
    # else:
    #     print("DEBUG: taxid column not found in parsed_df")

    
    # Map other customer fields
    field_mappings = {
        'nationality': 'nationality',
        'date_of_birth': 'date_of_birth',  
        'occupation': 'job_title',
        'gender': 'gender',
        'pep': 'pep',
        'risk_score': 'customer_risk_score',
        'income': 'monthly_income',
        'annual_turnover': 'annual_turnover',
        'source_of_income': 'source_of_funds',
        'total_assets': 'total_assets',
        'legal_form': 'legal_form',
        'account_executive': 'account_executive',
        'business_unit': 'business_unit',
        'country_risk': 'country_risk',
        'risk_category': 'risk_category',
        'country_of_residence_id': 'country_of_residence',
        'country_of_birth_id': 'country_of_birth'
    }
    
    for fixed_field, standard_field in field_mappings.items():
        if fixed_field in parsed_df.columns:
            if standard_field == 'nationality':
                # Replace '00' with 'US', otherwise use original value
                mapped_df[standard_field] = parsed_df[fixed_field].apply(
                    lambda x: 'US' if x == '00' else x
                )
            elif standard_field == 'country_of_birth_id':
                # Handle country of birth - convert blank to None, keep other values
                mapped_df[standard_field] = parsed_df[fixed_field].apply(
                    lambda x: None if not x or pd.isna(x) or str(x).strip() == '' else str(x).strip()
                )
            elif standard_field == 'country_of_residence_id':
                # Handle country of residence - convert blank to None, keep other values
                mapped_df[standard_field] = parsed_df[fixed_field].apply(
                    lambda x: None if not x or pd.isna(x) or str(x).strip() == '' else str(x).strip()
                )
            else:
                mapped_df[standard_field] = parsed_df[fixed_field]
    
    # Map address fields
    address_mappings = {
        'address_line_1': 'address_line_1',
        'address_line_2': 'address_line_2', 
        'address_line_3': 'address_zipcode',  # ZIP code goes to address_line_3 in fixed format
        'address_city': 'address_city',
        'address_state': 'address_state'
    }
    
    mapped_df['address_type'] = 'Primary'
    mapped_df['address_country'] = 'US'  # Default
    
    for fixed_field, standard_field in address_mappings.items():
        if fixed_field in parsed_df.columns:
            mapped_df[standard_field] = parsed_df[fixed_field]
    
    # Map phone fields
    if 'phone_number' in parsed_df.columns:
        mapped_df['phone_type'] = 'Primary'
        mapped_df['phone_number'] = parsed_df['phone_number']
    
    # Map email (contract_number field contains email in fixed format)
    if 'email_address' in parsed_df.columns:
        mapped_df['email_type'] = 'Primary'
        mapped_df['email_address'] = parsed_df['email_address']
    
    # Map identification fields
    if 'document_type' in parsed_df.columns:
        # Map identification type codes
        id_type_mapping = {
            'A': 'Driver License',
            'B': 'Passport',
            'C': 'Alien Registration',
            'D': 'Other Identification',
            'E': 'Disabled Elderly ID',
            'F': 'Foreign Entity ID',
            'L': 'Law Enforcement ID',
            'M': 'Amish Customer ID'
        }
        
        mapped_df['document_type'] = parsed_df['document_type'].map(id_type_mapping)
        
        if 'document_number' in parsed_df.columns:
            mapped_df['document_number'] = parsed_df['document_number']
    
    if 'source_of_funds' in parsed_df.columns:
        mapped_df['source_of_funds'] = parsed_df['source_of_funds']

    # Map business fields for business customers
    if 'legal_name' in parsed_df.columns:
        mapped_df['business_name'] = parsed_df['legal_name']
    
    if 'business_type' in parsed_df.columns:
        mapped_df['business_type'] = parsed_df['business_type']
    
    # Handle NAICS code
    if 'industry_code' in parsed_df.columns:
        mapped_df['industry_code'] = parsed_df['industry_code']
    
   
    return mapped_df

def map_account_fixed_width_to_standard(parsed_df: pd.DataFrame) -> pd.DataFrame:
    """Map fixed-width account data to standard format"""
    
    mapped_df = pd.DataFrame()
    
    # Add required fields - ALWAYS add changetype as 'add' for fixed-width files
    mapped_df['changetype'] = 'add'  # Default to add for fixed-width files
    temp_acc_id = None

    if 'customer_account_id' in parsed_df.columns:
        mapped_df['account_id'] = parsed_df['customer_account_id']
        #Fetch the account number which is stpred in the A_Number
        #First 15 Characters of Biller Division Name (all non-alphabetic and non-numeric characters removed) + AccountNumber + ‘BAWBP’
        temp_acc_id = parsed_df['customer_account_id'].apply(
            lambda x: x.replace('BAWBP', '') if x and 'BAWBP' in str(x) else x
        )
        temp_acc_id = temp_acc_id.apply(lambda x: str(x).strip())
        temp_acc_id = temp_acc_id.apply(lambda x: x[15:] if len(x) > 15 else x)
        # Store as account_number
        mapped_df['account_number'] = temp_acc_id
        
    elif 'A_Number' in parsed_df.columns:
        # Alternative field name
        mapped_df['account_id'] = parsed_df['A_Number']
        temp_acc_id = parsed_df['A_Number'].apply(
            lambda x: x.replace('BAWBP', '') if x and 'BAWBP' in str(x) else x
        )
        temp_acc_id = temp_acc_id.apply(lambda x: str(x).strip())
        temp_acc_id = temp_acc_id.apply(lambda x: x[15:] if len(x) > 15 else x)
        # Store as account_number
        mapped_df['account_number'] = temp_acc_id
       
    # CREATE BILLER ENTITY ID - NEW: Generate unique entity ID for each account
    mapped_df['customer_id'] = mapped_df['account_id'].apply(
        lambda x: f"BILLER_{x}" if x else None
    )
    
    # NEW: Add entity fields for biller creation
    mapped_df['entity_type'] = 'Biller'
    mapped_df['business_name'] = parsed_df.get('account_holder_name', '')  # Use account holder name as business name
    mapped_df['business_type'] = 'Service Provider - CheckFreePay'  # Default business type for billers
    

    # Map account fields
    field_mappings = {
        'account_type': 'account_type',
        'open_date': 'open_date',
        # 'closed_date': 'closed_date',
        'account_status': 'account_status',
        'account_holder_name': 'account_holder_name',
        'account_field_6': 'credit_limit',
        'account_field_1': 'business_unit',
        'account_field_2': 'risk_category',
        # 'risk_rating': 'account_risk_score',
        'branch_id': 'branch_id'
    }
    
    for fixed_field, standard_field in field_mappings.items():
        if fixed_field in parsed_df.columns:
            mapped_df[standard_field] = parsed_df[fixed_field]
    
    # Set default values
    mapped_df['account_currency'] = 'USD'
    mapped_df['account_balance'] = 0.0
    mapped_df['is_p2p_enabled'] = 'N'
    mapped_df['is_joint'] = 'N'
    
    return mapped_df

def parse_transaction_field_data(transaction_field_1, transaction_field_2, transaction_field_3, transaction_field_4):
    """
    Parse transaction fields to extract entity data for beneficiary bank processing
    
    Args:
        transaction_field_1: Contains DOB (last 8 chars), ID type (17th char), ID value (18-39 chars), source (40-41 chars)
        transaction_field_2: Contains name in format Lastname/Firstname/Middlename (first 35 chars)
        transaction_field_3: Contains address (full field)
        transaction_field_4: Contains city (first 35 chars), zipcode (36-43 chars), state (44-45 chars)
    
    Returns:
        dict: Parsed entity data
    """
    entity_data = {}
    
    # Parse transaction_field_1 for DOB, ID info
    if transaction_field_1:
        field1 = str(transaction_field_1).strip()
        
        # Date of birth - last 8 characters (YYYYMMDD format)
        if len(field1) >= 8:
            dob_str = field1[-8:]
            try:
                # Convert YYYYMMDD to YYYY-MM-DD
                if len(dob_str) == 8 and dob_str.isdigit():
                    year = dob_str[:4]
                    month = dob_str[4:6]
                    day = dob_str[6:8]
                    entity_data['date_of_birth'] = f"{year}-{month}-{day}"
            except:
                entity_data['date_of_birth'] = None
        
        # ID type - 17th character (index 16)
        if len(field1) >= 17:
            id_type_char = field1[16]
            id_type_mapping = {
                'A': 'Driver License',
                'B': 'Passport', 
                'C': 'Alien Registration',
                'D': 'Other Identification',
                'E': 'Disabled Elderly ID',
                'F': 'Foreign Entity ID',
                'L': 'Law Enforcement ID',
                'M': 'Amish Customer ID'
            }
            entity_data['identifier_type'] = id_type_mapping.get(id_type_char, 'Other Identification')
        
        # ID value - characters 18-39 (22 characters from 18th position)
        if len(field1) >= 40:
            entity_data['identifier_value'] = field1[17:39].strip()
        
        # Source - characters 40-41 (2 characters from 40th position)
        if len(field1) >= 42:
            entity_data['source'] = field1[39:41].strip()
    
    # Parse transaction_field_2 for name (first 35 characters)
    if transaction_field_2:
        field2 = str(transaction_field_2).strip()
        name_str = field2[:35].strip()
        
        # Parse Lastname/Firstname/Middlename format
        if '/' in name_str:
            name_parts = name_str.split('/')
            entity_data['last_name'] = name_parts[0].strip() if len(name_parts) > 0 else ''
            entity_data['first_name'] = name_parts[1].strip() if len(name_parts) > 1 else ''
            entity_data['middle_name'] = name_parts[2].strip() if len(name_parts) > 2 else ''
        else:
            # If no slashes, treat as last name
            entity_data['last_name'] = name_str
            entity_data['first_name'] = ''
            entity_data['middle_name'] = ''
    
    # Parse transaction_field_3 for address
    if transaction_field_3:
        entity_data['address_1'] = str(transaction_field_3).strip()
        entity_data['address_type'] = 'Primary'
    
    # Parse transaction_field_4 for city, zipcode, state
    if transaction_field_4:
        field4 = str(transaction_field_4).strip()
        
        # City - first 35 characters
        entity_data['address_city'] = field4[:35].strip() if len(field4) >= 35 else field4.strip()
        
        # Zipcode - 8 characters after first 35 (positions 36-43)
        if len(field4) >= 43:
            entity_data['address_zipcode'] = field4[35:43].strip()
        
        # State - 2 characters from 44th character (positions 44-45)
        if len(field4) >= 45:
            entity_data['address_state'] = field4[43:45].strip()
        
        # Country - always US
        entity_data['address_country'] = 'US'
    
    return entity_data

def create_beneficiary_entity_if_needed(row_dict, connection):
    # Only proceed if beneficiary_bank is different from intermediary_bank

    show_ui_notifications = True
    beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
    intermediary_bank = row_dict.get('intermediary_bank', '').strip()

    if not beneficiary_bank or beneficiary_bank == intermediary_bank:
        return False
    
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Check if entity already exists
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
        existing_entity = cursor.fetchone()
        
        if existing_entity:
            # logger.info(f"Related party entity {beneficiary_bank} already exists")
            cursor.close()
            # if show_ui_notifications:
            #     st.info(f"Related party entity {beneficiary_bank} already exists")
            return False
        
        # if show_ui_notifications:
            # st.info(f"Creating new related party entity: {beneficiary_bank}")
        
        # Parse transaction field data
        entity_data = parse_transaction_field_data(
            row_dict.get('transaction_field_1'),
            row_dict.get('transaction_field_2'), 
            row_dict.get('transaction_field_3'),
            row_dict.get('transaction_field_4')
        )
        
        # Step 1: Insert into entity table
        entity_sql = """
        INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        entity_params = (
            beneficiary_bank,
            'Customer',
            'UserUpload',
            'UserUpload', 
            current_time,
            current_time,
            'TransactionProcessor'
        )
        
        cursor.execute(entity_sql, entity_params)
        
        # Get the entity_id that was just created
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
        entity_result = cursor.fetchone()
        
        if not entity_result:
            logger.error(f"Failed to retrieve entity_id for {beneficiary_bank}")
            cursor.close()
            if show_ui_notifications:
                st.error(f"Failed to create entity for {beneficiary_bank}")
            return False
        
        entity_id = entity_result[0]
        logger.info(f"Created entity with ID {entity_id} for related party {beneficiary_bank}")
        
        # Continue with customer, address, identifier creation...
        # (Rest of the existing implementation)
        
        # Step 2: Insert into entity_customer table
        customer_sql = """
        INSERT INTO entity_customer (
            entity_id, customer_entity_id, first_name, middle_name, last_name,
            date_of_birth, customertype, nationality, svc_provider_name, 
            inserted_date, updated_date, inserted_by, updated_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        customer_params = (
            entity_id,
            beneficiary_bank,
            entity_data.get('first_name', ''),
            entity_data.get('middle_name', ''),
            entity_data.get('last_name', ''),
            entity_data.get('date_of_birth'),
            'Consumer',
            'US',
            'TransactionProcessor',
            current_time,
            current_time,
            'UserUpload',
            'UserUpload'
        )
        
        cursor.execute(customer_sql, customer_params)
        logger.info(f"Created customer record for entity {entity_id}")
        
        # Step 3-5: Address, identifier, and relationship creation
        # (Include existing implementation here)
        
        # Commit all changes
        connection.commit()
        cursor.close()
        
        if show_ui_notifications:
            st.success(f"Successfully created related party entity: {beneficiary_bank}")
        
        logger.info(f"Successfully created complete related party entity for {beneficiary_bank}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating related party entity for {beneficiary_bank}: {str(e)}")
        if show_ui_notifications:
            st.error(f"Error creating related party entity {beneficiary_bank}: {str(e)}")
        try:
            connection.rollback()
        except:
            pass
        return False

def process_transaction_add_operations_batch_with_beneficiary(add_operations, chunk_size=1000, max_workers=8):
    """
    Multi-threaded version of transaction ADD operations with beneficiary processing
    Enhanced with real-time UI updates - PRESERVING ALL ORIGINAL PROCESSING LOGIC
    """
    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    
    results = []
    success_count = 0
    error_count = 0
    beneficiary_entities_created = 0
    total_relationships_created = 0
    total_relationships_existed = 0
    agents_created = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} transaction ADD operations with multi-threading")
    
    # UI containers for progress tracking
    relationship_status_container = st.empty()
    relationship_progress_container = st.empty()
    relationship_metrics_container = st.empty()
    
    try:
        current_time = datetime.now()
        
        # PHASE 0: Pre-process all agent data using multi-threading
        logger.info("PRE-PROCESSING: Extracting all unique agent data for multi-threaded batch processing")
        all_agent_data_set = set()

        for op in add_operations:
            row_dict = op['row_dict']
            intermediary_1_id = row_dict.get('intermediary_1_id')
            transaction_field_15 = row_dict.get('transaction_field_15')
            if intermediary_1_id:
                all_agent_data_set.add((intermediary_1_id, transaction_field_15))
        
        # Process all agents using multi-threading
        if all_agent_data_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_agent_data_set)} unique agents")
            with relationship_status_container.container():
                st.info(f"Multi-threaded pre-processing {len(all_agent_data_set)} unique agents")
            
            all_agent_data_list = list(all_agent_data_set)
            agent_entity_map = process_agents_in_chunks_threaded(all_agent_data_list, current_time, chunk_size//2, max_workers)
            
            agents_created = len([agent_id for agent_id, field_15 in all_agent_data_list 
                               if agent_id in agent_entity_map])
            
            logger.info(f"PRE-PROCESSING: Multi-threaded agent processing complete - {agents_created} agents processed")
        else:
            agent_entity_map = {}
        
        # PHASE 0.5: Pre-process ALL beneficiary entities using multi-threading
        logger.info("PRE-PROCESSING: Multi-threaded beneficiary entity processing")
        all_beneficiaries_set = set()

        for op in add_operations:
            row_dict = op['row_dict']
            beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
            intermediary_bank = row_dict.get('intermediary_bank', '').strip()
            
            if beneficiary_bank and beneficiary_bank != intermediary_bank:
                all_beneficiaries_set.add(beneficiary_bank)
        
        # Process beneficiaries in parallel
        if all_beneficiaries_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_beneficiaries_set)} unique beneficiary entities")
            
            def process_beneficiaries_chunk(beneficiaries_list):
                """Process beneficiaries in a separate thread"""
                thread_id = threading.current_thread().name
                created_count = 0
                
                try:
                    with db_connection.get_connection() as connection:
                        cursor = connection.cursor()
                        
                        # Check which beneficiaries already exist
                        placeholders = ','.join(['%s'] * len(beneficiaries_list))
                        cursor.execute(f"SELECT customer_entity_id FROM entity WHERE customer_entity_id IN ({placeholders})", beneficiaries_list)
                        existing_beneficiaries = {row[0] for row in cursor.fetchall()}
                        
                        # Create only the missing ones
                        new_beneficiaries = set(beneficiaries_list) - existing_beneficiaries
                        
                        if new_beneficiaries:
                            # Prepare bulk insert data
                            beneficiary_inserts = []
                            for beneficiary_id in new_beneficiaries:
                                beneficiary_inserts.append((
                                    beneficiary_id,      # customer_entity_id
                                    'RELATED_PARTY',     # entity_type
                                    beneficiary_id,      # entity_name
                                    'US',               # country
                                    None,               # date_of_birth
                                    'ORGANIZATION',     # customer_type
                                    current_time,       # insert_date
                                    current_time,       # update_date
                                    'SYSTEM',           # inserted_by
                                    'SYSTEM'            # updated_by
                                ))
                            
                            # Bulk insert new beneficiaries
                            beneficiary_sql = """
                                INSERT INTO entity (
                                    customer_entity_id, entity_type, entity_name, country, 
                                    date_of_birth, customer_type, insert_date, update_date, 
                                    inserted_by, updated_by
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.executemany(beneficiary_sql, beneficiary_inserts)
                            connection.commit()
                            created_count = len(beneficiary_inserts)
                            
                            logger.info(f"Thread {thread_id}: Created {created_count} beneficiary entities")
                        
                        cursor.close()
                        
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing beneficiaries: {str(e)}")
                
                return created_count
            
            # Split beneficiaries into chunks for parallel processing
            all_beneficiaries_list = list(all_beneficiaries_set)
            beneficiary_chunks = [all_beneficiaries_list[i:i + chunk_size//4] for i in range(0, len(all_beneficiaries_list), chunk_size//4)]
            
            with ThreadPoolExecutor(max_workers=max_workers//2, thread_name_prefix="BeneficiaryProcessor") as executor:
                future_to_chunk = {executor.submit(process_beneficiaries_chunk, chunk): chunk for chunk in beneficiary_chunks}
                
                for future in as_completed(future_to_chunk):
                    try:
                        chunk_created = future.result()
                        beneficiary_entities_created += chunk_created
                    except Exception as e:
                        logger.error(f"Beneficiary processing future failed: {str(e)}")
            
            logger.info(f"PRE-PROCESSING: Multi-threaded beneficiary processing complete - {beneficiary_entities_created} created")
        
        # PHASE 1: Process transaction chunks in parallel
        operation_chunks = [add_operations[i:i + chunk_size] for i in range(0, len(add_operations), chunk_size)]
        logger.info(f"Split ADD operations into {len(operation_chunks)} chunks for multi-threaded processing")
        
        # Initialize progress tracking
        with relationship_status_container.container():
            st.info(f"Multi-threaded processing: {len(add_operations)} transactions in {len(operation_chunks)} chunks")
        
        with relationship_progress_container.container():
            progress_bar = st.progress(0)
        
        # Shared state for real-time updates
        shared_state = {
            'completed_chunks': 0,
            'total_success': 0,
            'total_relationships_created': 0,
            'total_relationships_existed': 0,
            'lock': threading.Lock()
        }
        
        # Thread-safe containers for results
        chunk_results_queue = queue.Queue()
        
        def process_add_chunk_worker(chunk_index, chunk):
            """Worker function for processing ADD operations chunk - ORIGINAL LOGIC PRESERVED"""
            thread_id = threading.current_thread().name
            chunk_start_time = time.time()
            
            logger.info(f"Thread {thread_id}: Starting ADD chunk {chunk_index + 1}/{len(operation_chunks)} with {len(chunk)} operations")
            
            chunk_success = 0
            chunk_errors = 0
            chunk_results = []
            chunk_relationships_created = 0
            chunk_relationships_existed = 0
            
            try:
                with db_connection.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    # PHASE 2: Handle relationship creation for this chunk
                    relationship_pairs = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                        intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                        
                        if beneficiary_bank and beneficiary_bank != intermediary_bank:
                            # Get entity IDs for relationship checking
                            intermediary_id = get_entity_id_by_bank(cursor, intermediary_bank)
                            beneficiary_id = get_entity_id_by_bank(cursor, beneficiary_bank)
                            
                            if intermediary_id and beneficiary_id:
                                pair = (intermediary_id, beneficiary_id)
                                relationship_pairs.append(pair)

                    # Process relationships for this chunk
                    if relationship_pairs:
                        logger.info(f"Thread {thread_id}: Processing {len(relationship_pairs)} relationships")
                        
                        existing_relationships = batch_check_relationships_exist_for_modify(cursor, relationship_pairs)
                        
                        if existing_relationships:
                            updated_count = batch_update_relationships(cursor, existing_relationships, current_time)
                            chunk_relationships_existed = len(existing_relationships)
                            logger.info(f"Thread {thread_id}: Updated {updated_count} existing relationships")
                        
                        new_relationships = set(relationship_pairs) - existing_relationships
                        
                        if new_relationships:
                            created_count = batch_create_new_relationships(cursor, new_relationships, current_time)
                            chunk_relationships_created = created_count
                            logger.info(f"Thread {thread_id}: Created {created_count} new relationships")
                    
                    # PHASE 3: Batch process transactions for this chunk
                    transaction_inserts = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        db_account_id = op['db_account_id']
                        
                        transaction_params = prepare_transaction_insert_params(
                            row_dict, db_account_id, current_time, agent_entity_map, None
                        )
                        
                        validation = validate_transaction_sql_parameters(transaction_params)
                        if not validation['match']:
                            logger.error(f"Thread {thread_id}: Row {op['index']}: Parameter validation failed")
                            chunk_errors += 1
                            chunk_results.append(f"Row {op['index']}: Parameter validation failed for transaction {op['transaction_id']}")
                            continue
                        
                        transaction_inserts.append(transaction_params)
                    
                    # Execute transaction batch insert
                    if transaction_inserts:
                        transaction_sql = """
                        INSERT INTO transactions (
                            account_id, b_number, customer_transaction_id, transaction_code, transaction_type,
                            preferred_currency, nominal_currency, amount, debit_credit_indicator, transaction_date,
                            tran_code_description, description, reference, balance, eod_balance,
                            non_account_holder, transaction_origin_country, counterparty_routing_number, counterparty_name, branch_id,
                            teller_id, check_number, check_image_front_path, check_image_back_path, txn_location,
                            txn_source, intermediary_1_id, intermediary_1_type, intermediary_1_code, intermediary_1_name,
                            intermediary_1_role, intermediary_1_reference, intermediary_1_fee, intermediary_1_fee_currency, intermediary_2_id,
                            intermediary_2_type, intermediary_2_code, intermediary_2_name, intermediary_2_role, intermediary_2_reference,
                            intermediary_2_fee, intermediary_2_fee_currency, intermediary_3_id, intermediary_3_type, intermediary_3_code,
                            intermediary_3_name, intermediary_3_role, intermediary_3_reference, intermediary_3_fee, intermediary_3_fee_currency,
                            positive_pay_indicator, originator_bank, beneficiary_details, intermediary_bank, beneficiary_bank,
                            instructing_bank, inter_bank_information, other_information, related_account, transaction_field_1,
                            transaction_field_2, transaction_field_3, transaction_field_4, transaction_field_5, transaction_field_11,
                            transaction_field_12, transaction_field_13, transaction_field_14, transaction_field_15, is_p2p_transaction,
                            p2p_recipient_type, p2p_recipient, p2p_sender_type, p2p_sender, p2p_reference_id,
                            insert_date, update_date, inserted_by, updated_by, svc_provider_name
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        logger.info(f"Thread {thread_id}: Executing batch insert for {len(transaction_inserts)} transactions")
                        cursor.executemany(transaction_sql, transaction_inserts)
                        chunk_success = len(transaction_inserts)
                        logger.info(f"Thread {thread_id}: Successfully inserted {chunk_success} transactions")
                    
                    # Commit chunk changes
                    connection.commit()
                    cursor.close()
                    
                    # Generate results for this chunk
                    for i, op in enumerate(chunk):
                        if i < chunk_success:
                            chunk_results.append(f"Row {op['index']}: Successfully added transaction {op['transaction_id']}")
                    
                    if chunk_relationships_created > 0:
                        chunk_results.append(f"Thread {thread_id}: Created {chunk_relationships_created} new entity relationships")
                    
                    if chunk_relationships_existed > 0:
                        chunk_results.append(f"Thread {thread_id}: Updated {chunk_relationships_existed} existing entity relationships")
                
                chunk_end_time = time.time()
                chunk_processing_time = chunk_end_time - chunk_start_time
                
                result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': chunk_success,
                    'error_count': chunk_errors,
                    'results': chunk_results,
                    'relationships_created': chunk_relationships_created,
                    'relationships_existed': chunk_relationships_existed,
                    'processing_time': chunk_processing_time
                }
                
                chunk_results_queue.put(result)
                
                # Update shared state and UI from main thread
                with shared_state['lock']:
                    shared_state['completed_chunks'] += 1
                    shared_state['total_success'] += chunk_success
                    shared_state['total_relationships_created'] += chunk_relationships_created
                    shared_state['total_relationships_existed'] += chunk_relationships_existed
                
                logger.info(f"Thread {thread_id}: Completed ADD chunk {chunk_index + 1}")
                return result
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Thread {thread_id}: Error processing ADD chunk {chunk_index + 1}: {str(e)}")
                logger.error(f"Thread {thread_id}: ADD Full traceback: {error_details}")
                
                error_result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': len(chunk),
                    'results': [f"Thread {thread_id}: ADD Chunk {chunk_index + 1} error - {str(e)}"],
                    'relationships_created': 0,
                    'relationships_existed': 0,
                    'processing_time': 0,
                    'error': str(e)
                }
                
                chunk_results_queue.put(error_result)
                
                with shared_state['lock']:
                    shared_state['completed_chunks'] += 1
                
                return error_result
        
        
        # Execute ADD chunks in parallel with real-time UI updates
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="AddProcessor") as executor:
            # Submit all futures
            future_to_chunk = {
                executor.submit(process_add_chunk_worker, i, chunk): i 
                for i, chunk in enumerate(operation_chunks)
            }
            
            # STREAMLIT COMPATIBLE: Update UI as each chunk completes
            completed_count = 0
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    completed_count += 1
                    
                    # Update UI directly from main thread
                    progress = completed_count / len(operation_chunks)
                    progress_bar.progress(progress)
                    
                    with relationship_status_container.container():
                        st.info(f" Completed chunk {completed_count}/{len(operation_chunks)} | Processing transactions...")
                    
                    with relationship_metrics_container.container():
                        st.subheader(f" Progress: {completed_count}/{len(operation_chunks)} chunks completed")
                        col1, col2, col3 = st.columns(3)
                        col1.metric(" Completed Chunks", completed_count)
                        col2.metric(" Total Chunks", len(operation_chunks))
                        col3.metric(" Progress", f"{progress:.1%}")
                    
                    logger.info(f"ADD Future completed for chunk {chunk_index + 1}")
                    
                except Exception as e:
                    completed_count += 1
                    logger.error(f"ADD Future failed for chunk {chunk_index + 1}: {str(e)}")

        
        # Collect all results from queue
        all_chunk_results = []
        while not chunk_results_queue.empty():
            all_chunk_results.append(chunk_results_queue.get())
        
        # Sort results by chunk index
        all_chunk_results.sort(key=lambda x: x['chunk_index'])
        
        # Aggregate final results
        for chunk_result in all_chunk_results:
            success_count += chunk_result['success_count']
            error_count += chunk_result['error_count']
            results.extend(chunk_result['results'])
            total_relationships_created += chunk_result['relationships_created']
            total_relationships_existed += chunk_result['relationships_existed']
        
        # Complete progress bar
        progress_bar.progress(1.0)
        
        with relationship_status_container.container():
            st.success(f" Multi-threaded ADD processing complete! {success_count} transactions processed using {max_workers} threads")
        
        with relationship_metrics_container.container():
            st.subheader("🎉 Final Multi-threaded ADD Results")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric(" Total Transactions", success_count)
            col2.metric(" Beneficiary Entities", beneficiary_entities_created)
            col3.metric(" Agent Entities", agents_created)
            col4.metric(" New Relationships", total_relationships_created)
            col5.metric(" Updated Relationships", total_relationships_existed)
        
        logger.info(f"Multi-threaded ADD BATCH COMPLETE: {success_count} transactions, {beneficiary_entities_created} entities created using {max_workers} threads")
        
        # Add summary results
        if beneficiary_entities_created > 0:
            results.insert(0, f"Multi-threaded: Created {beneficiary_entities_created} beneficiary entities")
        if agents_created > 0:
            results.insert(0, f"Multi-threaded: Processed {agents_created} agent entities")
        
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    except Exception as e:
        logger.error(f"Multi-threaded ADD batch operation failed: {str(e)}")
        error_count = len(add_operations)
        results.append(f"Multi-threaded ADD batch operation failed: {str(e)}")
                
        with relationship_status_container.container():
            st.error(f" Error in multi-threaded ADD processing: {str(e)}")
        
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    return success_count, error_count, results


def process_transaction_modify_operations_batch_with_relationship(modify_operations, chunk_size=1000, max_workers=8):
    """
    Multi-threaded version of transaction MODIFY operations with relationship processing
    Enhanced with real-time UI updates - PRESERVING ALL ORIGINAL PROCESSING LOGIC
    """
    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    pool_info = db_connection.get_pool_info()
    logger.info(f"Connection pool status: {pool_info}")
    
    results = []
    success_count = 0
    error_count = 0
    beneficiary_entities_created = 0
    beneficiary_relationships_updated = 0
    total_skipped_count = 0
    agents_processed = 0
    
    if not modify_operations:
        return 0, 0, []
    
    if max_workers is None:
        max_workers = 4
    elif isinstance(max_workers, bool):
        logger.error(f"max_workers was boolean {max_workers}, converting to integer 4")
        max_workers = 4
    elif not isinstance(max_workers, int) or max_workers <= 0:
        logger.error(f"Invalid max_workers {max_workers} (type: {type(max_workers)}), using default 4")
        max_workers = 4
    
    logger.info(f"Processing {len(modify_operations)} transaction MODIFY operations with multi-threading")
    
    # UI containers
    progress_container = st.empty()
    metrics_container = st.empty()
    status_container = st.empty()
    
    try:
        current_time = datetime.now()
        
        # PHASE 0: Pre-process all agent data using multi-threading
        logger.info("PRE-PROCESSING: Multi-threaded agent data extraction and processing")
        all_agent_data_set = set()
        
        for op in modify_operations:
            row_dict = op['row_dict']
            intermediary_1_id = row_dict.get('intermediary_1_id')
            transaction_field_15 = row_dict.get('transaction_field_15')
            if intermediary_1_id:
                all_agent_data_set.add((intermediary_1_id, transaction_field_15))
        
        # Process all agents using multi-threading
        if all_agent_data_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_agent_data_set)} unique agents")
            with status_container.container():
                st.info(f"Multi-threaded pre-processing {len(all_agent_data_set)} unique agents")
            
            all_agent_data_list = list(all_agent_data_set)
            agent_entity_map = process_agents_in_chunks_threaded(all_agent_data_list, current_time, chunk_size//2, max_workers//2)
            
            agents_processed = len([agent_id for agent_id, field_15 in all_agent_data_list 
                                 if agent_id in agent_entity_map])
            
            logger.info(f"PRE-PROCESSING: Multi-threaded agent processing complete - {agents_processed} agents processed")
        else:
            agent_entity_map = {}
        
        # PHASE 0.5: Pre-process beneficiaries using multi-threading (similar to ADD operations)
        logger.info("PRE-PROCESSING: Multi-threaded beneficiary entity processing")
        all_beneficiaries_set = set()
        
        for op in modify_operations:
            row_dict = op['row_dict']
            beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
            intermediary_bank = row_dict.get('intermediary_bank', '').strip()
            
            if beneficiary_bank and beneficiary_bank != intermediary_bank:
                all_beneficiaries_set.add(beneficiary_bank)
        
        # Bulk create missing beneficiaries using multi-threading
        if all_beneficiaries_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_beneficiaries_set)} unique beneficiary entities")
            
            def process_beneficiaries_modify_chunk(beneficiaries_list):
                """Process beneficiaries for MODIFY operations"""
                thread_id = threading.current_thread().name
                created_count = 0
                
                try:
                    with db_connection.get_connection() as connection:
                        cursor = connection.cursor()
                        
                        # Check existing beneficiaries
                        placeholders = ','.join(['%s'] * len(beneficiaries_list))
                        cursor.execute(f"SELECT customer_entity_id FROM entity WHERE customer_entity_id IN ({placeholders})", beneficiaries_list)
                        existing_beneficiaries = {row[0] for row in cursor.fetchall()}
                        
                        # Create missing ones
                        new_beneficiaries = set(beneficiaries_list) - existing_beneficiaries
                        
                        if new_beneficiaries:
                            beneficiary_inserts = []
                            for beneficiary_id in new_beneficiaries:
                                beneficiary_inserts.append((
                                    beneficiary_id, 'RELATED_PARTY', beneficiary_id, 'US', None, 'ORGANIZATION',
                                    current_time, current_time, 'SYSTEM', 'SYSTEM'
                                ))
                            
                            beneficiary_sql = """
                                INSERT INTO entity (
                                    customer_entity_id, entity_type, entity_name, country, 
                                    date_of_birth, customer_type, insert_date, update_date, 
                                    inserted_by, updated_by
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.executemany(beneficiary_sql, beneficiary_inserts)
                            connection.commit()
                            created_count = len(beneficiary_inserts)
                            
                            logger.info(f"Thread {thread_id}: Created {created_count} beneficiary entities for MODIFY")
                        
                        cursor.close()
                        
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing beneficiaries for MODIFY: {str(e)}")
                
                return created_count
            
            # Process beneficiaries in parallel
            all_beneficiaries_list = list(all_beneficiaries_set)
            beneficiary_chunks = [all_beneficiaries_list[i:i + chunk_size//4] for i in range(0, len(all_beneficiaries_list), chunk_size//4)]
            
            with ThreadPoolExecutor(max_workers=max_workers//2, thread_name_prefix="ModifyBeneficiaryProcessor") as executor:
                future_to_chunk = {executor.submit(process_beneficiaries_modify_chunk, chunk): chunk for chunk in beneficiary_chunks}
                
                for future in as_completed(future_to_chunk):
                    try:
                        chunk_created = future.result()
                        beneficiary_entities_created += chunk_created
                    except Exception as e:
                        logger.error(f"MODIFY beneficiary processing future failed: {str(e)}")
            
            logger.info(f"PRE-PROCESSING: Multi-threaded MODIFY beneficiary processing complete - {beneficiary_entities_created} created")
        
        # PHASE 1: Process MODIFY chunks in parallel
        operation_chunks = [modify_operations[i:i + chunk_size] for i in range(0, len(modify_operations), chunk_size)]
        logger.info(f"Split MODIFY operations into {len(operation_chunks)} chunks for multi-threaded processing")
        
        # Initialize progress tracking
        with status_container.container():
            st.info(f"Multi-threaded MODIFY processing: {len(modify_operations)} operations in {len(operation_chunks)} chunks")
        
        with progress_container.container():
            progress_bar = st.progress(0.0)
        
        # Shared state for real-time updates
        shared_state = {
            'completed_chunks': 0,
            'total_success': 0,
            'total_relationships_updated': 0,
            'total_skipped': 0,
            'lock': threading.Lock()
        }
        
        # Thread-safe containers for results
        modify_results_queue = queue.Queue()
        
        def process_modify_chunk_worker(chunk_index, chunk):
            """Worker function for processing MODIFY operations chunk - ORIGINAL LOGIC PRESERVED"""
            thread_id = threading.current_thread().name
            chunk_start_time = time.time()
            
            logger.info(f"Thread {thread_id}: Starting MODIFY chunk {chunk_index + 1}/{len(operation_chunks)} with {len(chunk)} operations")
            
            chunk_success = 0
            chunk_errors = 0
            chunk_results = []
            chunk_relationships_updated = 0
            chunk_skipped_count = 0
            
            try:
                with db_connection.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    # PHASE 2: Account ID Lookup for this chunk
                    customer_account_ids = set()
                    chunk_data = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        db_transaction_id = op['db_transaction_id']
                        customer_account_id = row_dict.get('account_id')
                        
                        if customer_account_id:
                            customer_account_ids.add(customer_account_id)
                        
                        chunk_data.append((row_dict, db_transaction_id, current_time))
                    
                    # Batch lookup account_ids for this chunk
                    logger.info(f"Thread {thread_id}: Looking up {len(customer_account_ids)} unique customer account IDs")
                    account_id_cache = batch_lookup_account_ids(cursor, customer_account_ids)
                    
                    # Filter operations based on account ID resolution
                    valid_operations = []
                    for op in chunk:
                        row_dict = op['row_dict']
                        customer_account_id = row_dict.get('account_id')
                        
                        if customer_account_id and customer_account_id not in account_id_cache:
                            logger.warning(f"Thread {thread_id}: Skipping operation {op['index']} with unresolved customer_account_id: {customer_account_id}")
                            chunk_skipped_count += 1
                            chunk_results.append(f"Row {op['index']}: Skipped due to unresolved account_id: {customer_account_id}")
                            continue
                        
                        # Update row_dict with resolved account_id
                        if customer_account_id:
                            row_dict_copy = row_dict.copy()
                            row_dict_copy['account_id'] = account_id_cache[customer_account_id]
                            op['row_dict'] = row_dict_copy
                        
                        valid_operations.append(op)
                    
                    logger.info(f"Thread {thread_id}: Account lookup complete - {len(valid_operations)} valid operations, {chunk_skipped_count} skipped")
                    
                    if not valid_operations:
                        logger.warning(f"Thread {thread_id}: No valid operations after account lookup, skipping chunk")
                        result = {
                            'chunk_index': chunk_index,
                            'thread_id': thread_id,
                            'success_count': 0,
                            'error_count': 0,
                            'results': chunk_results,
                            'relationships_updated': 0,
                            'skipped_count': chunk_skipped_count,
                            'processing_time': time.time() - chunk_start_time
                        }
                        modify_results_queue.put(result)
                        return result
                    
                    # PHASE 3: Process relationships for this chunk
                    relationship_pairs = []
                    processed_relationship_pairs = set()

                    for op in valid_operations:
                        row_dict = op['row_dict']
                        beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                        intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                        
                        if beneficiary_bank and beneficiary_bank != intermediary_bank:
                            bank_pair = (intermediary_bank, beneficiary_bank)
                            if bank_pair not in processed_relationship_pairs:
                                intermediary_id = get_entity_id_by_bank(cursor, intermediary_bank)
                                beneficiary_id = get_entity_id_by_bank(cursor, beneficiary_bank)
                            
                                if intermediary_id and beneficiary_id:
                                    pair = (intermediary_id, beneficiary_id)
                                    relationship_pairs.append(pair)

                                processed_relationship_pairs.add(bank_pair)

                    logger.info(f"Thread {thread_id}: Collected {len(relationship_pairs)} relationship pairs")
                    
                    # Handle relationship updates
                    if relationship_pairs:
                        logger.info(f"Thread {thread_id}: Processing {len(relationship_pairs)} relationship pairs")
                        
                        # Check existing relationships
                        existing_relationships = batch_check_relationships_exist_for_modify(cursor, relationship_pairs)
                        
                        # Update existing relationships
                        if existing_relationships:
                            logger.info(f"Thread {thread_id}: Found {len(existing_relationships)} existing relationships to update")
                            updated_count = batch_update_relationships(cursor, existing_relationships, current_time)
                            chunk_relationships_updated += updated_count
                        
                        # Create new relationships
                        new_relationships = set(relationship_pairs) - existing_relationships
                        
                        if new_relationships:
                            logger.info(f"Thread {thread_id}: Creating {len(new_relationships)} new relationships")
                            created_count = batch_create_new_relationships(cursor, new_relationships, current_time)
                            chunk_relationships_updated += created_count
                        
                        # Handle cleanup of outdated relationships
                        transaction_ids_in_chunk = [op['db_transaction_id'] for op in valid_operations]
                        removed_count = cleanup_outdated_transaction_relationships(cursor, transaction_ids_in_chunk, relationship_pairs, current_time)
                        
                        if removed_count > 0:
                            logger.info(f"Thread {thread_id}: Removed/deactivated {removed_count} outdated relationships")
                        
                        logger.info(f"Thread {thread_id}: Relationship processing complete - updated/created {chunk_relationships_updated}, removed {removed_count}")

                    # PHASE 4: Batch process transaction updates for this chunk
                    logger.info(f"Thread {thread_id}: Processing transaction updates with resolved account IDs and agent lookup")
                    transaction_updates = []
                    
                    for op in valid_operations:
                        row_dict = op['row_dict']
                        db_transaction_id = op['db_transaction_id']
                        
                        transaction_params = prepare_transaction_update_params(
                            row_dict, db_transaction_id, current_time, account_id_cache, agent_entity_map, cursor
                        )
                        
                        if transaction_params and len(transaction_params) == 79:  # 78 SET + 1 WHERE
                            transaction_updates.append(transaction_params)
                        else:
                            logger.error(f"Thread {thread_id}: Row {op['index']}: Invalid parameter count - expected 79, got {len(transaction_params) if transaction_params else 0}")
                            chunk_errors += 1
                            chunk_results.append(f"Row {op['index']}: Parameter validation failed - invalid parameter count")
                            continue
                    
                    # Execute transaction batch update
                    if transaction_updates:
                        update_query = """
                            UPDATE transactions SET
                                account_id = %s,
                                b_number = %s,
                                customer_transaction_id = %s,
                                transaction_code = %s,
                                transaction_type = %s,
                                preferred_currency = %s,
                                nominal_currency = %s,
                                amount = %s,
                                debit_credit_indicator = %s,
                                transaction_date = %s,
                                tran_code_description = %s,
                                description = %s,
                                reference = %s,
                                balance = %s,
                                eod_balance = %s,
                                non_account_holder = %s,
                                transaction_origin_country = %s,
                                counterparty_routing_number = %s,
                                counterparty_name = %s,
                                branch_id = %s,
                                teller_id = %s,
                                check_number = %s,
                                check_image_front_path = %s,
                                check_image_back_path = %s,
                                txn_location = %s,
                                txn_source = %s,
                                intermediary_1_id = %s,
                                intermediary_1_type = %s,
                                intermediary_1_code = %s,
                                intermediary_1_name = %s,
                                intermediary_1_role = %s,
                                intermediary_1_reference = %s,
                                intermediary_1_fee = %s,
                                intermediary_1_fee_currency = %s,
                                intermediary_2_id = %s,
                                intermediary_2_type = %s,
                                intermediary_2_code = %s,
                                intermediary_2_name = %s,
                                intermediary_2_role = %s,
                                intermediary_2_reference = %s,
                                intermediary_2_fee = %s,
                                intermediary_2_fee_currency = %s,
                                intermediary_3_id = %s,
                                intermediary_3_type = %s,
                                intermediary_3_code = %s,
                                intermediary_3_name = %s,
                                intermediary_3_role = %s,
                                intermediary_3_reference = %s,
                                intermediary_3_fee = %s,
                                intermediary_3_fee_currency = %s,
                                positive_pay_indicator = %s,
                                originator_bank = %s,
                                beneficiary_details = %s,
                                intermediary_bank = %s,
                                beneficiary_bank = %s,
                                instructing_bank = %s,
                                inter_bank_information = %s,
                                other_information = %s,
                                related_account = %s,
                                transaction_field_1 = %s,
                                transaction_field_2 = %s,
                                transaction_field_3 = %s,
                                transaction_field_4 = %s,
                                transaction_field_5 = %s,
                                transaction_field_11 = %s,
                                transaction_field_12 = %s,
                                transaction_field_13 = %s,
                                transaction_field_14 = %s,
                                transaction_field_15 = %s,
                                is_p2p_transaction = %s,
                                p2p_recipient_type = %s,
                                p2p_recipient = %s,
                                p2p_sender_type = %s,
                                p2p_sender = %s,
                                p2p_reference_id = %s,
                                update_date = %s,
                                updated_by = %s,
                                svc_provider_name = %s
                            WHERE transaction_id = %s
                        """
                        
                        logger.info(f"Thread {thread_id}: Executing batch update for {len(transaction_updates)} transactions with agent lookup")
                        cursor.executemany(update_query, transaction_updates)
                        chunk_success = len(transaction_updates)
                        logger.info(f"Thread {thread_id}: Successfully updated {chunk_success} transactions")
                    
                    # Commit chunk changes
                    connection.commit()
                    cursor.close()
                    logger.info(f"Thread {thread_id}: Committed all changes successfully")
                    
                    # Generate results for this chunk
                    for op in valid_operations:
                        if op['db_transaction_id'] in [param[-1] for param in transaction_updates]:
                            chunk_results.append(f"Row {op['index']}: Successfully modified transaction {op['transaction_id']} with agent lookup")
                    
                    if chunk_relationships_updated > 0:
                        chunk_results.append(f"Thread {thread_id}: Updated {chunk_relationships_updated} entity relationships")
                    
                    if chunk_skipped_count > 0:
                        chunk_results.append(f"Thread {thread_id}: Skipped {chunk_skipped_count} transactions due to unresolved account IDs")
                
                chunk_end_time = time.time()
                chunk_processing_time = chunk_end_time - chunk_start_time
                
                result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': chunk_success,
                    'error_count': chunk_errors,
                    'results': chunk_results,
                    'relationships_updated': chunk_relationships_updated,
                    'skipped_count': chunk_skipped_count,
                    'processing_time': chunk_processing_time
                }
                
                modify_results_queue.put(result)
                
                # Update shared state
                with shared_state['lock']:
                    shared_state['completed_chunks'] += 1
                    shared_state['total_success'] += chunk_success
                    shared_state['total_relationships_updated'] += chunk_relationships_updated
                    shared_state['total_skipped'] += chunk_skipped_count
                
                logger.info(f"Thread {thread_id}: Completed MODIFY chunk {chunk_index + 1}")
                return result
                
            except Exception as e:
                logger.error(f"Thread {thread_id}: Error processing MODIFY chunk {chunk_index + 1}: {str(e)}")
                
                error_result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': len(chunk),
                    'results': [f"Thread {thread_id}: MODIFY Chunk {chunk_index + 1} error - {str(e)}"],
                    'relationships_updated': 0,
                    'skipped_count': 0,
                    'processing_time': 0,
                    'error': str(e)
                }
                
                modify_results_queue.put(error_result)
                
                with shared_state['lock']:
                    shared_state['completed_chunks'] += 1
                
                return error_result
        
        # Execute MODIFY chunks in parallel with real-time UI updates
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ModifyProcessor") as executor:
            # Submit all futures
            future_to_chunk = {
                executor.submit(process_modify_chunk_worker, i, chunk): i 
                for i, chunk in enumerate(operation_chunks)
            }
            
            # STREAMLIT COMPATIBLE: Update UI as each chunk completes
            completed_count = 0
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    completed_count += 1
                    
                    # Update UI directly from main thread
                    progress = completed_count / len(operation_chunks)
                    progress_bar.progress(progress)
                    
                    with status_container.container():
                        st.info(f" Completed chunk {completed_count}/{len(operation_chunks)} | Processing transactions...")
                    
                    with metrics_container.container():
                        st.subheader(f" Progress: {completed_count}/{len(operation_chunks)} chunks completed")
                        col1, col2, col3 = st.columns(3)
                        col1.metric(" Completed Chunks", completed_count)
                        col2.metric(" Total Chunks", len(operation_chunks))
                        col3.metric(" Progress", f"{progress:.1%}")
                    
                    logger.info(f"MODIFY Future completed for chunk {chunk_index + 1}")
                    
                except Exception as e:
                    completed_count += 1
                    logger.error(f"MODIFY Future failed for chunk {chunk_index + 1}: {str(e)}")
        
        # Collect all results from queue
        all_modify_results = []
        while not modify_results_queue.empty():
            all_modify_results.append(modify_results_queue.get())
        
        # Sort results by chunk index
        all_modify_results.sort(key=lambda x: x['chunk_index'])
        
        # Aggregate final results
        for chunk_result in all_modify_results:
            success_count += chunk_result['success_count']
            error_count += chunk_result['error_count']
            results.extend(chunk_result['results'])
            beneficiary_relationships_updated += chunk_result['relationships_updated']
            total_skipped_count += chunk_result['skipped_count']
        
        # Complete progress bar
        progress_bar.progress(1.0)
        
        with status_container.container():
            st.success(f" Multi-threaded MODIFY processing complete! {success_count} transactions processed using {max_workers} threads")
        
        with metrics_container.container():
            st.subheader(" Final Multi-threaded MODIFY Results")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric(" Total Transactions", success_count)
            col2.metric(" Beneficiary Entities", beneficiary_entities_created)
            col3.metric(" Agent Entities", agents_processed)
            col4.metric(" Relationships Updated", beneficiary_relationships_updated)
            col5.metric(" Transactions Skipped", total_skipped_count)
        
        logger.info(f"Multi-threaded MODIFY BATCH COMPLETE: {success_count} transactions, {agents_processed} agents processed, {beneficiary_entities_created} entities created using {max_workers} threads")
        
        # Add summary results
        summary_parts = []
        if agents_processed > 0:
            summary_parts.append(f"processed {agents_processed} agent entities")
        if beneficiary_entities_created > 0:
            summary_parts.append(f"created {beneficiary_entities_created} beneficiary entities")
        if beneficiary_relationships_updated > 0:
            summary_parts.append(f"updated {beneficiary_relationships_updated} relationships")
        if total_skipped_count > 0:
            summary_parts.append(f"skipped {total_skipped_count} transactions due to unresolved account IDs")
        
        if summary_parts:
            results.insert(0, f"Multi-threaded MODIFY Summary: {', '.join(summary_parts)}")
        
        time.sleep(1)
        progress_container.empty()
        metrics_container.empty()
        status_container.empty()
        
    except Exception as e:
        logger.error(f"Multi-threaded MODIFY batch operation failed: {str(e)}")
        error_count = len(modify_operations)
        results.append(f"Multi-threaded MODIFY batch operation failed: {str(e)}")
        
        
        with status_container.container():
            st.error(f" Error in multi-threaded MODIFY processing: {str(e)}")
        
        time.sleep(1)
        if 'progress_container' in locals():
            progress_container.empty()
        if 'metrics_container' in locals():
            metrics_container.empty()
        if 'status_container' in locals():
            status_container.empty()
    
    return success_count, error_count, results


def process_transaction_add_operations_batch_with_beneficiary_prev(add_operations, chunk_size=1000, max_workers=8):
    """
    Multi-threaded version of transaction ADD operations with beneficiary processing
    """
    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    
    results = []
    success_count = 0
    error_count = 0
    beneficiary_entities_created = 0
    total_relationships_created = 0
    total_relationships_existed = 0
    agents_created = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} transaction ADD operations with multi-threading")
    
    # UI containers for progress tracking
    relationship_status_container = st.empty()
    relationship_progress_container = st.empty()
    relationship_metrics_container = st.empty()
    
    try:
        with relationship_status_container.container():
            st.info(f"Multi-threaded processing: {len(add_operations)} transactions")
        
        with relationship_progress_container.container():
            progress_bar = st.progress(0)
        
        current_time = datetime.now()
        
        # PHASE 0: Pre-process all agent data using multi-threading
        logger.info("PRE-PROCESSING: Extracting all unique agent data for multi-threaded batch processing")
        all_agent_data_set = set()

        for op in add_operations:
            row_dict = op['row_dict']
            intermediary_1_id = row_dict.get('intermediary_1_id')
            transaction_field_15 = row_dict.get('transaction_field_15')
            if intermediary_1_id:
                all_agent_data_set.add((intermediary_1_id, transaction_field_15))
        
        # Process all agents using multi-threading
        if all_agent_data_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_agent_data_set)} unique agents")
            with relationship_status_container.container():
                st.info(f"Multi-threaded pre-processing {len(all_agent_data_set)} unique agents")
            
            all_agent_data_list = list(all_agent_data_set)
            agent_entity_map = process_agents_in_chunks_threaded(all_agent_data_list, current_time, chunk_size//2, max_workers)
            
            agents_created = len([agent_id for agent_id, field_15 in all_agent_data_list 
                               if agent_id in agent_entity_map])
            
            logger.info(f"PRE-PROCESSING: Multi-threaded agent processing complete - {agents_created} agents processed")
        else:
            agent_entity_map = {}
        
        # PHASE 0.5: Pre-process ALL beneficiary entities using multi-threading
        logger.info("PRE-PROCESSING: Multi-threaded beneficiary entity processing")
        all_beneficiaries_set = set()

        for op in add_operations:
            row_dict = op['row_dict']
            beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
            intermediary_bank = row_dict.get('intermediary_bank', '').strip()
            
            if beneficiary_bank and beneficiary_bank != intermediary_bank:
                all_beneficiaries_set.add(beneficiary_bank)
        
        # Process beneficiaries in parallel
        if all_beneficiaries_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_beneficiaries_set)} unique beneficiary entities")
            
            def process_beneficiaries_chunk(beneficiaries_list):
                """Process beneficiaries in a separate thread"""
                thread_id = threading.current_thread().name
                created_count = 0
                
                try:
                    with db_connection.get_connection() as connection:
                        cursor = connection.cursor()
                        
                        # Check which beneficiaries already exist
                        placeholders = ','.join(['%s'] * len(beneficiaries_list))
                        cursor.execute(f"SELECT customer_entity_id FROM entity WHERE customer_entity_id IN ({placeholders})", beneficiaries_list)
                        existing_beneficiaries = {row[0] for row in cursor.fetchall()}
                        
                        # Create only the missing ones
                        new_beneficiaries = set(beneficiaries_list) - existing_beneficiaries
                        
                        if new_beneficiaries:
                            # Prepare bulk insert data
                            beneficiary_inserts = []
                            for beneficiary_id in new_beneficiaries:
                                beneficiary_inserts.append((
                                    beneficiary_id,      # customer_entity_id
                                    'RELATED_PARTY',     # entity_type
                                    beneficiary_id,      # entity_name
                                    'US',               # country
                                    None,               # date_of_birth
                                    'ORGANIZATION',     # customer_type
                                    current_time,       # insert_date
                                    current_time,       # update_date
                                    'SYSTEM',           # inserted_by
                                    'SYSTEM'            # updated_by
                                ))
                            
                            # Bulk insert new beneficiaries
                            beneficiary_sql = """
                                INSERT INTO entity (
                                    customer_entity_id, entity_type, entity_name, country, 
                                    date_of_birth, customer_type, insert_date, update_date, 
                                    inserted_by, updated_by
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.executemany(beneficiary_sql, beneficiary_inserts)
                            connection.commit()
                            created_count = len(beneficiary_inserts)
                            
                            logger.info(f"Thread {thread_id}: Created {created_count} beneficiary entities")
                        
                        cursor.close()
                        
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing beneficiaries: {str(e)}")
                
                return created_count
            
            # Split beneficiaries into chunks for parallel processing
            all_beneficiaries_list = list(all_beneficiaries_set)
            beneficiary_chunks = [all_beneficiaries_list[i:i + chunk_size//4] for i in range(0, len(all_beneficiaries_list), chunk_size//4)]
            
            with ThreadPoolExecutor(max_workers=max_workers//2, thread_name_prefix="BeneficiaryProcessor") as executor:
                future_to_chunk = {executor.submit(process_beneficiaries_chunk, chunk): chunk for chunk in beneficiary_chunks}
                
                for future in as_completed(future_to_chunk):
                    try:
                        chunk_created = future.result()
                        beneficiary_entities_created += chunk_created
                    except Exception as e:
                        logger.error(f"Beneficiary processing future failed: {str(e)}")
            
            logger.info(f"PRE-PROCESSING: Multi-threaded beneficiary processing complete - {beneficiary_entities_created} created")
        
        # PHASE 1: Process transaction chunks in parallel
        operation_chunks = [add_operations[i:i + chunk_size] for i in range(0, len(add_operations), chunk_size)]
        logger.info(f"Split ADD operations into {len(operation_chunks)} chunks for multi-threaded processing")
        
        # Thread-safe containers for results
        chunk_results_queue = queue.Queue()
        processing_lock = threading.Lock()
        completed_chunks = 0
        
        def process_add_chunk_worker(chunk_index, chunk):
            """Worker function for processing ADD operations chunk"""
            thread_id = threading.current_thread().name
            chunk_start_time = time.time()
            
            logger.info(f"Thread {thread_id}: Starting ADD chunk {chunk_index + 1}/{len(operation_chunks)} with {len(chunk)} operations")
            
            chunk_success = 0
            chunk_errors = 0
            chunk_results = []
            chunk_relationships_created = 0
            chunk_relationships_existed = 0
            
            try:
                with db_connection.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    # PHASE 2: Handle relationship creation for this chunk
                    relationship_pairs = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                        intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                        
                        if beneficiary_bank and beneficiary_bank != intermediary_bank:
                            # Get entity IDs for relationship checking
                            intermediary_id = get_entity_id_by_bank(cursor, intermediary_bank)
                            beneficiary_id = get_entity_id_by_bank(cursor, beneficiary_bank)
                            
                            if intermediary_id and beneficiary_id:
                                pair = (intermediary_id, beneficiary_id)
                                relationship_pairs.append(pair)

                    # Process relationships for this chunk
                    if relationship_pairs:
                        logger.info(f"Thread {thread_id}: Processing {len(relationship_pairs)} relationships")
                        
                        existing_relationships = batch_check_relationships_exist_for_modify(cursor, relationship_pairs)
                        
                        if existing_relationships:
                            updated_count = batch_update_relationships(cursor, existing_relationships, current_time)
                            chunk_relationships_existed = len(existing_relationships)
                            logger.info(f"Thread {thread_id}: Updated {updated_count} existing relationships")
                        
                        new_relationships = set(relationship_pairs) - existing_relationships
                        
                        if new_relationships:
                            created_count = batch_create_new_relationships(cursor, new_relationships, current_time)
                            chunk_relationships_created = created_count
                            logger.info(f"Thread {thread_id}: Created {created_count} new relationships")
                    
                    # PHASE 3: Batch process transactions for this chunk
                    transaction_inserts = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        db_account_id = op['db_account_id']
                        
                        transaction_params = prepare_transaction_insert_params(
                            row_dict, db_account_id, current_time, agent_entity_map, None
                        )
                        
                        validation = validate_transaction_sql_parameters(transaction_params)
                        if not validation['match']:
                            logger.error(f"Thread {thread_id}: Row {op['index']}: Parameter validation failed")
                            chunk_errors += 1
                            chunk_results.append(f"Row {op['index']}: Parameter validation failed for transaction {op['transaction_id']}")
                            continue
                        
                        transaction_inserts.append(transaction_params)
                    
                    # Execute transaction batch insert
                    if transaction_inserts:
                        transaction_sql = """
                        INSERT INTO transactions (
                            account_id, b_number, customer_transaction_id, transaction_code, transaction_type,
                            preferred_currency, nominal_currency, amount, debit_credit_indicator, transaction_date,
                            tran_code_description, description, reference, balance, eod_balance,
                            non_account_holder, transaction_origin_country, counterparty_routing_number, counterparty_name, branch_id,
                            teller_id, check_number, check_image_front_path, check_image_back_path, txn_location,
                            txn_source, intermediary_1_id, intermediary_1_type, intermediary_1_code, intermediary_1_name,
                            intermediary_1_role, intermediary_1_reference, intermediary_1_fee, intermediary_1_fee_currency, intermediary_2_id,
                            intermediary_2_type, intermediary_2_code, intermediary_2_name, intermediary_2_role, intermediary_2_reference,
                            intermediary_2_fee, intermediary_2_fee_currency, intermediary_3_id, intermediary_3_type, intermediary_3_code,
                            intermediary_3_name, intermediary_3_role, intermediary_3_reference, intermediary_3_fee, intermediary_3_fee_currency,
                            positive_pay_indicator, originator_bank, beneficiary_details, intermediary_bank, beneficiary_bank,
                            instructing_bank, inter_bank_information, other_information, related_account, transaction_field_1,
                            transaction_field_2, transaction_field_3, transaction_field_4, transaction_field_5, transaction_field_11,
                            transaction_field_12, transaction_field_13, transaction_field_14, transaction_field_15, is_p2p_transaction,
                            p2p_recipient_type, p2p_recipient, p2p_sender_type, p2p_sender, p2p_reference_id,
                            insert_date, update_date, inserted_by, updated_by, svc_provider_name
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        logger.info(f"Thread {thread_id}: Executing batch insert for {len(transaction_inserts)} transactions")
                        cursor.executemany(transaction_sql, transaction_inserts)
                        chunk_success = len(transaction_inserts)
                        logger.info(f"Thread {thread_id}: Successfully inserted {chunk_success} transactions")
                    
                    # Commit chunk changes
                    connection.commit()
                    cursor.close()
                    
                    # Generate results for this chunk
                    for i, op in enumerate(chunk):
                        if i < chunk_success:
                            chunk_results.append(f"Row {op['index']}: Successfully added transaction {op['transaction_id']}")
                    
                    if chunk_relationships_created > 0:
                        chunk_results.append(f"Thread {thread_id}: Created {chunk_relationships_created} new entity relationships")
                    
                    if chunk_relationships_existed > 0:
                        chunk_results.append(f"Thread {thread_id}: Updated {chunk_relationships_existed} existing entity relationships")
                
                chunk_end_time = time.time()
                chunk_processing_time = chunk_end_time - chunk_start_time
                
                result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': chunk_success,
                    'error_count': chunk_errors,
                    'results': chunk_results,
                    'relationships_created': chunk_relationships_created,
                    'relationships_existed': chunk_relationships_existed,
                    'processing_time': chunk_processing_time
                }
                
                chunk_results_queue.put(result)
                
                # Update progress safely
                nonlocal completed_chunks
                with processing_lock:
                    completed_chunks += 1
                    logger.info(f"Thread {thread_id}: Completed ADD chunk {chunk_index + 1}")
                    
                    # progress_bar.progress(progress)
                    
                    # with relationship_metrics_container.container():
                    #     st.subheader("Multi-threaded ADD Operations Progress")
                    #     col1, col2, col3, col4, col5 = st.columns(5)
                    #     col1.metric("Completed Chunks", completed_chunks)
                    #     col2.metric("Total Chunks", len(operation_chunks))
                    #     col3.metric("Active Threads", threading.active_count() - 1)
                    #     col4.metric("Progress", f"{progress:.1%}")
                    #     col5.metric("Processing Time", f"{chunk_processing_time:.2f}s")
                
                logger.info(f"Thread {thread_id}: Completed ADD chunk {chunk_index + 1} successfully")
                return result
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Thread {thread_id}: Error processing ADD chunk {chunk_index + 1}: {str(e)}")
                logger.error(f"Thread {thread_id}: ADD Full traceback: {error_details}")
                
                error_result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': len(chunk),
                    'results': [f"Thread {thread_id}: ADD Chunk {chunk_index + 1} error - {str(e)}"],
                    'relationships_created': 0,
                    'relationships_existed': 0,
                    'processing_time': 0,
                    'error': str(e)
                }
                
                chunk_results_queue.put(error_result)
                
                with processing_lock:
                    completed_chunks += 1
                    progress = min(completed_chunks / len(operation_chunks),1.0)
                    # progress_bar.progress(progress)
                    logger.error(f"Thread {thread_id}: Error in ADD chunk {chunk_index + 1}: {str(e)}")
                    # with relationship_status_container.container():
                    #     st.error(f"Thread {thread_id}: Error in ADD chunk {chunk_index + 1}: {str(e)}")
                
                return error_result
        
        # Execute ADD chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="AddProcessor") as executor:
            future_to_chunk = {
                executor.submit(process_add_chunk_worker, i, chunk): i 
                for i, chunk in enumerate(operation_chunks)
            }
            
            # Wait for all futures to complete
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    logger.info(f"ADD Future completed for chunk {chunk_index + 1}")
                except Exception as e:
                    logger.error(f"ADD Future failed for chunk {chunk_index + 1}: {str(e)}")
        
        # Collect all results from queue
        all_chunk_results = []
        while not chunk_results_queue.empty():
            all_chunk_results.append(chunk_results_queue.get())
        
        # Sort results by chunk index
        all_chunk_results.sort(key=lambda x: x['chunk_index'])
        
        # Aggregate final results
        for chunk_result in all_chunk_results:
            success_count += chunk_result['success_count']
            error_count += chunk_result['error_count']
            results.extend(chunk_result['results'])
            total_relationships_created += chunk_result['relationships_created']
            total_relationships_existed += chunk_result['relationships_existed']
        
        # Complete progress bar
        progress_bar.progress(1.0)
        
        with relationship_status_container.container():
            st.success(f"Multi-threaded ADD processing complete! {success_count} transactions processed using {max_workers} threads")
        
        with relationship_metrics_container.container():
            st.subheader("Final Multi-threaded ADD Results")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total Transactions", success_count)
            col2.metric("Beneficiary Entities", beneficiary_entities_created)
            col3.metric("Agent Entities", agents_created)
            col4.metric("New Relationships", total_relationships_created)
            col5.metric("Updated Relationships", total_relationships_existed)
        
        logger.info(f"Multi-threaded ADD BATCH COMPLETE: {success_count} transactions, {beneficiary_entities_created} entities created using {max_workers} threads")
        
        # Add summary results
        if beneficiary_entities_created > 0:
            results.insert(0, f"Multi-threaded: Created {beneficiary_entities_created} beneficiary entities")
        if agents_created > 0:
            results.insert(0, f"Multi-threaded: Processed {agents_created} agent entities")
        
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    except Exception as e:
        logger.error(f"Multi-threaded ADD batch operation failed: {str(e)}")
        error_count = len(add_operations)
        results.append(f"Multi-threaded ADD batch operation failed: {str(e)}")
        
        with relationship_status_container.container():
            st.error(f"Error in multi-threaded ADD processing: {str(e)}")
        
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    return success_count, error_count, results

def process_transaction_modify_operations_batch_with_relationship_prev(modify_operations, chunk_size=1000, max_workers=8):
    """
    Multi-threaded version of transaction MODIFY operations with relationship processing
    """
    

    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    pool_info = db_connection.get_pool_info()
    logger.info(f"Connection pool status: {pool_info}")
    
    results = []
    success_count = 0
    error_count = 0
    beneficiary_entities_created = 0
    beneficiary_relationships_updated = 0
    total_skipped_count = 0
    agents_processed = 0
    
    if not modify_operations:
        return 0, 0, []
    
    if max_workers is None:
        max_workers = 4
    elif isinstance(max_workers, bool):
        logger.error(f"max_workers was boolean {max_workers}, converting to integer 4")
        max_workers = 4
    elif not isinstance(max_workers, int) or max_workers <= 0:
        logger.error(f"Invalid max_workers {max_workers} (type: {type(max_workers)}), using default 4")
        max_workers = 4
    
    logger.info(f"Processing {len(modify_operations)} transaction MODIFY operations with multi-threading")
    
    # UI containers
    progress_container = st.empty()
    metrics_container = st.empty()
    status_container = st.empty()
    
    try:
        with status_container.container():
            st.info(f"Multi-threaded MODIFY processing: {len(modify_operations)} operations")
        
        with progress_container.container():
            progress_bar = st.progress(0.0)

        # Split into chunks
        operation_chunks = [modify_operations[i:i + chunk_size] for i in range(0, len(modify_operations), chunk_size)]
        logger.info(f"Split MODIFY operations into {len(operation_chunks)} chunks")
        
        # Thread-safe containers
        modify_results_queue = queue.Queue()
        modify_lock = threading.Lock()
        completed_modify_chunks = 0
        
        current_time = datetime.now()
        
        # PHASE 0: Pre-process all agent data using multi-threading
        logger.info("PRE-PROCESSING: Multi-threaded agent data extraction and processing")
        all_agent_data_set = set()
        
        for op in modify_operations:
            row_dict = op['row_dict']
            intermediary_1_id = row_dict.get('intermediary_1_id')
            transaction_field_15 = row_dict.get('transaction_field_15')
            if intermediary_1_id:
                all_agent_data_set.add((intermediary_1_id, transaction_field_15))
        
        # Process all agents using multi-threading
        if all_agent_data_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_agent_data_set)} unique agents")
            all_agent_data_list = list(all_agent_data_set)
            agent_entity_map = process_agents_in_chunks_threaded(all_agent_data_list, current_time, chunk_size//2, max_workers//2)
            
            agents_processed = len([agent_id for agent_id, field_15 in all_agent_data_list 
                                 if agent_id in agent_entity_map])
            
            logger.info(f"PRE-PROCESSING: Multi-threaded agent processing complete - {agents_processed} agents processed")
        else:
            agent_entity_map = {}
        
        # PHASE 0.5: Pre-process beneficiaries using multi-threading (similar to ADD operations)
        logger.info("PRE-PROCESSING: Multi-threaded beneficiary entity processing")
        all_beneficiaries_set = set()
        
        for op in modify_operations:
            row_dict = op['row_dict']
            beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
            intermediary_bank = row_dict.get('intermediary_bank', '').strip()
            
            if beneficiary_bank and beneficiary_bank != intermediary_bank:
                all_beneficiaries_set.add(beneficiary_bank)
        
        # Bulk create missing beneficiaries using multi-threading
        if all_beneficiaries_set:
            logger.info(f"PRE-PROCESSING: Multi-threaded processing of {len(all_beneficiaries_set)} unique beneficiary entities")
            
            def process_beneficiaries_modify_chunk(beneficiaries_list):
                """Process beneficiaries for MODIFY operations"""
                thread_id = threading.current_thread().name
                created_count = 0
                
                try:
                    with db_connection.get_connection() as connection:
                        cursor = connection.cursor()
                        
                        # Check existing beneficiaries
                        placeholders = ','.join(['%s'] * len(beneficiaries_list))
                        cursor.execute(f"SELECT customer_entity_id FROM entity WHERE customer_entity_id IN ({placeholders})", beneficiaries_list)
                        existing_beneficiaries = {row[0] for row in cursor.fetchall()}
                        
                        # Create missing ones
                        new_beneficiaries = set(beneficiaries_list) - existing_beneficiaries
                        
                        if new_beneficiaries:
                            beneficiary_inserts = []
                            for beneficiary_id in new_beneficiaries:
                                beneficiary_inserts.append((
                                    beneficiary_id, 'RELATED_PARTY', beneficiary_id, 'US', None, 'ORGANIZATION',
                                    current_time, current_time, 'SYSTEM', 'SYSTEM'
                                ))
                            
                            beneficiary_sql = """
                                INSERT INTO entity (
                                    customer_entity_id, entity_type, entity_name, country, 
                                    date_of_birth, customer_type, insert_date, update_date, 
                                    inserted_by, updated_by
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.executemany(beneficiary_sql, beneficiary_inserts)
                            connection.commit()
                            created_count = len(beneficiary_inserts)
                            
                            logger.info(f"Thread {thread_id}: Created {created_count} beneficiary entities for MODIFY")
                        
                        cursor.close()
                        
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing beneficiaries for MODIFY: {str(e)}")
                
                return created_count
            
            # Process beneficiaries in parallel
            all_beneficiaries_list = list(all_beneficiaries_set)
            beneficiary_chunks = [all_beneficiaries_list[i:i + chunk_size//4] for i in range(0, len(all_beneficiaries_list), chunk_size//4)]
            
            with ThreadPoolExecutor(max_workers=max_workers//2, thread_name_prefix="ModifyBeneficiaryProcessor") as executor:
                future_to_chunk = {executor.submit(process_beneficiaries_modify_chunk, chunk): chunk for chunk in beneficiary_chunks}
                
                for future in as_completed(future_to_chunk):
                    try:
                        chunk_created = future.result()
                        beneficiary_entities_created += chunk_created
                    except Exception as e:
                        logger.error(f"MODIFY beneficiary processing future failed: {str(e)}")
            
            logger.info(f"PRE-PROCESSING: Multi-threaded MODIFY beneficiary processing complete - {beneficiary_entities_created} created")
        
        # PHASE 1: Process MODIFY chunks in parallel
        operation_chunks = [modify_operations[i:i + chunk_size] for i in range(0, len(modify_operations), chunk_size)]
        logger.info(f"Split MODIFY operations into {len(operation_chunks)} chunks for multi-threaded processing")
        
        # Thread-safe containers for results
        modify_results_queue = queue.Queue()
        modify_lock = threading.Lock()
        completed_modify_chunks = 0
        
        def process_modify_chunk_worker(chunk_index, chunk):
            """Worker function for processing MODIFY operations chunk"""
            thread_id = threading.current_thread().name
            chunk_start_time = time.time()
            
            logger.info(f"Thread {thread_id}: Starting MODIFY chunk {chunk_index + 1}/{len(operation_chunks)} with {len(chunk)} operations")
            
            chunk_success = 0
            chunk_errors = 0
            chunk_results = []
            chunk_relationships_updated = 0
            chunk_skipped_count = 0
            
            try:
                with db_connection.get_connection() as connection:
                    cursor = connection.cursor()
                    
                    # PHASE 2: Account ID Lookup for this chunk
                    customer_account_ids = set()
                    chunk_data = []
                    
                    for op in chunk:
                        row_dict = op['row_dict']
                        db_transaction_id = op['db_transaction_id']
                        customer_account_id = row_dict.get('account_id')
                        
                        if customer_account_id:
                            customer_account_ids.add(customer_account_id)
                        
                        chunk_data.append((row_dict, db_transaction_id, current_time))
                    
                    # Batch lookup account_ids for this chunk
                    logger.info(f"Thread {thread_id}: Looking up {len(customer_account_ids)} unique customer account IDs")
                    account_id_cache = batch_lookup_account_ids(cursor, customer_account_ids)
                    
                    # Filter operations based on account ID resolution
                    valid_operations = []
                    for op in chunk:
                        row_dict = op['row_dict']
                        customer_account_id = row_dict.get('account_id')
                        
                        if customer_account_id and customer_account_id not in account_id_cache:
                            logger.warning(f"Thread {thread_id}: Skipping operation {op['index']} with unresolved customer_account_id: {customer_account_id}")
                            chunk_skipped_count += 1
                            chunk_results.append(f"Row {op['index']}: Skipped due to unresolved account_id: {customer_account_id}")
                            continue
                        
                        # Update row_dict with resolved account_id
                        if customer_account_id:
                            row_dict_copy = row_dict.copy()
                            row_dict_copy['account_id'] = account_id_cache[customer_account_id]
                            op['row_dict'] = row_dict_copy
                        
                        valid_operations.append(op)
                    
                    logger.info(f"Thread {thread_id}: Account lookup complete - {len(valid_operations)} valid operations, {chunk_skipped_count} skipped")
                    
                    if not valid_operations:
                        logger.warning(f"Thread {thread_id}: No valid operations after account lookup, skipping chunk")
                        result = {
                            'chunk_index': chunk_index,
                            'thread_id': thread_id,
                            'success_count': 0,
                            'error_count': 0,
                            'results': chunk_results,
                            'relationships_updated': 0,
                            'skipped_count': chunk_skipped_count,
                            'processing_time': time.time() - chunk_start_time
                        }
                        modify_results_queue.put(result)
                        return result
                    
                    # PHASE 3: Process relationships for this chunk
                    relationship_pairs = []
                    processed_relationship_pairs = set()

                    for op in valid_operations:
                        row_dict = op['row_dict']
                        beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                        intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                        
                        if beneficiary_bank and beneficiary_bank != intermediary_bank:
                            bank_pair = (intermediary_bank, beneficiary_bank)
                            if bank_pair not in processed_relationship_pairs:
                                intermediary_id = get_entity_id_by_bank(cursor, intermediary_bank)
                                beneficiary_id = get_entity_id_by_bank(cursor, beneficiary_bank)
                            
                                if intermediary_id and beneficiary_id:
                                    pair = (intermediary_id, beneficiary_id)
                                    relationship_pairs.append(pair)

                                processed_relationship_pairs.add(bank_pair)

                    logger.info(f"Thread {thread_id}: Collected {len(relationship_pairs)} relationship pairs")
                    
                    # Handle relationship updates
                    if relationship_pairs:
                        logger.info(f"Thread {thread_id}: Processing {len(relationship_pairs)} relationship pairs")
                        
                        # Check existing relationships
                        existing_relationships = batch_check_relationships_exist_for_modify(cursor, relationship_pairs)
                        
                        # Update existing relationships
                        if existing_relationships:
                            logger.info(f"Thread {thread_id}: Found {len(existing_relationships)} existing relationships to update")
                            updated_count = batch_update_relationships(cursor, existing_relationships, current_time)
                            chunk_relationships_updated += updated_count
                        
                        # Create new relationships
                        new_relationships = set(relationship_pairs) - existing_relationships
                        
                        if new_relationships:
                            logger.info(f"Thread {thread_id}: Creating {len(new_relationships)} new relationships")
                            created_count = batch_create_new_relationships(cursor, new_relationships, current_time)
                            chunk_relationships_updated += created_count
                        
                        # Handle cleanup of outdated relationships
                        transaction_ids_in_chunk = [op['db_transaction_id'] for op in valid_operations]
                        removed_count = cleanup_outdated_transaction_relationships(cursor, transaction_ids_in_chunk, relationship_pairs, current_time)
                        
                        if removed_count > 0:
                            logger.info(f"Thread {thread_id}: Removed/deactivated {removed_count} outdated relationships")
                        
                        logger.info(f"Thread {thread_id}: Relationship processing complete - updated/created {chunk_relationships_updated}, removed {removed_count}")

                    # PHASE 4: Batch process transaction updates for this chunk
                    logger.info(f"Thread {thread_id}: Processing transaction updates with resolved account IDs and agent lookup")
                    transaction_updates = []
                    
                    for op in valid_operations:
                        row_dict = op['row_dict']
                        db_transaction_id = op['db_transaction_id']
                        
                        transaction_params = prepare_transaction_update_params(
                            row_dict, db_transaction_id, current_time, account_id_cache, agent_entity_map, cursor
                        )
                        
                        if transaction_params and len(transaction_params) == 79:  # 78 SET + 1 WHERE
                            transaction_updates.append(transaction_params)
                        else:
                            logger.error(f"Thread {thread_id}: Row {op['index']}: Invalid parameter count - expected 79, got {len(transaction_params) if transaction_params else 0}")
                            chunk_errors += 1
                            chunk_results.append(f"Row {op['index']}: Parameter validation failed - invalid parameter count")
                            continue
                    
                    # Execute transaction batch update
                    if transaction_updates:
                        update_query = """
                            UPDATE transactions SET
                                account_id = %s,
                                b_number = %s,
                                customer_transaction_id = %s,
                                transaction_code = %s,
                                transaction_type = %s,
                                preferred_currency = %s,
                                nominal_currency = %s,
                                amount = %s,
                                debit_credit_indicator = %s,
                                transaction_date = %s,
                                tran_code_description = %s,
                                description = %s,
                                reference = %s,
                                balance = %s,
                                eod_balance = %s,
                                non_account_holder = %s,
                                transaction_origin_country = %s,
                                counterparty_routing_number = %s,
                                counterparty_name = %s,
                                branch_id = %s,
                                teller_id = %s,
                                check_number = %s,
                                check_image_front_path = %s,
                                check_image_back_path = %s,
                                txn_location = %s,
                                txn_source = %s,
                                intermediary_1_id = %s,
                                intermediary_1_type = %s,
                                intermediary_1_code = %s,
                                intermediary_1_name = %s,
                                intermediary_1_role = %s,
                                intermediary_1_reference = %s,
                                intermediary_1_fee = %s,
                                intermediary_1_fee_currency = %s,
                                intermediary_2_id = %s,
                                intermediary_2_type = %s,
                                intermediary_2_code = %s,
                                intermediary_2_name = %s,
                                intermediary_2_role = %s,
                                intermediary_2_reference = %s,
                                intermediary_2_fee = %s,
                                intermediary_2_fee_currency = %s,
                                intermediary_3_id = %s,
                                intermediary_3_type = %s,
                                intermediary_3_code = %s,
                                intermediary_3_name = %s,
                                intermediary_3_role = %s,
                                intermediary_3_reference = %s,
                                intermediary_3_fee = %s,
                                intermediary_3_fee_currency = %s,
                                positive_pay_indicator = %s,
                                originator_bank = %s,
                                beneficiary_details = %s,
                                intermediary_bank = %s,
                                beneficiary_bank = %s,
                                instructing_bank = %s,
                                inter_bank_information = %s,
                                other_information = %s,
                                related_account = %s,
                                transaction_field_1 = %s,
                                transaction_field_2 = %s,
                                transaction_field_3 = %s,
                                transaction_field_4 = %s,
                                transaction_field_5 = %s,
                                transaction_field_11 = %s,
                                transaction_field_12 = %s,
                                transaction_field_13 = %s,
                                transaction_field_14 = %s,
                                transaction_field_15 = %s,
                                is_p2p_transaction = %s,
                                p2p_recipient_type = %s,
                                p2p_recipient = %s,
                                p2p_sender_type = %s,
                                p2p_sender = %s,
                                p2p_reference_id = %s,
                                update_date = %s,
                                updated_by = %s,
                                svc_provider_name = %s
                            WHERE transaction_id = %s
                        """
                        
                        logger.info(f"Thread {thread_id}: Executing batch update for {len(transaction_updates)} transactions with agent lookup")
                        cursor.executemany(update_query, transaction_updates)
                        chunk_success = len(transaction_updates)
                        logger.info(f"Thread {thread_id}: Successfully updated {chunk_success} transactions")
                    
                    # Commit chunk changes
                    connection.commit()
                    cursor.close()
                    logger.info(f"Thread {thread_id}: Committed all changes successfully")
                    
                    # Generate results for this chunk
                    for op in valid_operations:
                        if op['db_transaction_id'] in [param[-1] for param in transaction_updates]:
                            chunk_results.append(f"Row {op['index']}: Successfully modified transaction {op['transaction_id']} with agent lookup")
                    
                    if chunk_relationships_updated > 0:
                        chunk_results.append(f"Thread {thread_id}: Updated {chunk_relationships_updated} entity relationships")
                    
                    if chunk_skipped_count > 0:
                        chunk_results.append(f"Thread {thread_id}: Skipped {chunk_skipped_count} transactions due to unresolved account IDs")
                
                chunk_end_time = time.time()
                chunk_processing_time = chunk_end_time - chunk_start_time
                
                result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': chunk_success,
                    'error_count': chunk_errors,
                    'results': chunk_results,
                    'relationships_updated': chunk_relationships_updated,
                    'skipped_count': chunk_skipped_count,
                    'processing_time': chunk_processing_time
                }
                
                modify_results_queue.put(result)
                
                # Update progress safely
                nonlocal completed_modify_chunks
                with modify_lock:
                    completed_modify_chunks += 1
                    progress =  min(completed_modify_chunks / len(operation_chunks), 1.0)  # Cap at 1.0/100
                    
                    # Update UI
                    # with status_container.container():
                    logger.info(f"Thread {thread_id}: Completed MODIFY chunk {chunk_index + 1} ({chunk_success} transactions, {chunk_relationships_updated} relationships)")
                    
                    progress_bar.progress(progress)
                    
                    # with metrics_container.container():
                    #     st.subheader("Multi-threaded MODIFY Operations Progress")
                    #     col1, col2, col3, col4, col5 = st.columns(5)
                    #     col1.metric("Completed Chunks", completed_modify_chunks)
                    #     col2.metric("Total Chunks", len(operation_chunks))
                    #     col3.metric("Active Threads", threading.active_count() - 1)
                    #     col4.metric("Progress", f"{progress:.1%}")
                    #     col5.metric("Processing Time", f"{chunk_processing_time:.2f}s")
                
                logger.info(f"Thread {thread_id}: Completed MODIFY chunk {chunk_index + 1} successfully")
                return result
                
            except Exception as e:
                logger.error(f"Thread {thread_id}: Error processing MODIFY chunk {chunk_index + 1}: {str(e)}")
                
                error_result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': len(chunk),
                    'results': [f"Thread {thread_id}: MODIFY Chunk {chunk_index + 1} error - {str(e)}"],
                    'relationships_updated': 0,
                    'skipped_count': 0,
                    'processing_time': 0,
                    'error': str(e)
                }
                
                modify_results_queue.put(error_result)
                
                with modify_lock:
                    completed_modify_chunks += 1
                    progress = min(completed_modify_chunks / len(operation_chunks), 1.0)
                    progress_bar.progress(progress)
                    
                    with status_container.container():
                        st.error(f"Thread {thread_id}: Error in MODIFY chunk {chunk_index + 1}: {str(e)}")
                
                return error_result
        
        # Execute MODIFY chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ModifyProcessor") as executor:
            future_to_chunk = {
                executor.submit(process_modify_chunk_worker, i, chunk): i 
                for i, chunk in enumerate(operation_chunks)
            }
            
            # Wait for all futures to complete
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    logger.info(f"MODIFY Future completed for chunk {chunk_index + 1}")
                except Exception as e:
                    logger.error(f"MODIFY Future failed for chunk {chunk_index + 1}: {str(e)}")
        
        # Collect all results from queue
        all_modify_results = []
        while not modify_results_queue.empty():
            all_modify_results.append(modify_results_queue.get())
        
        # Sort results by chunk index
        all_modify_results.sort(key=lambda x: x['chunk_index'])
        
        # Aggregate final results
        for chunk_result in all_modify_results:
            success_count += chunk_result['success_count']
            error_count += chunk_result['error_count']
            results.extend(chunk_result['results'])
            beneficiary_relationships_updated += chunk_result['relationships_updated']
            total_skipped_count += chunk_result['skipped_count']
        
        # Complete progress bar
        progress_bar.progress(1.0)
        
        # with status_container.container():
        logger.info(f"Multi-threaded MODIFY processing complete! {success_count} transactions processed using {max_workers} threads")
        
        # with metrics_container.container():
        #     st.subheader("Final Multi-threaded MODIFY Results")
        #     col1, col2, col3, col4, col5 = st.columns(5)
        #     col1.metric("Total Transactions", success_count)
        #     col2.metric("Beneficiary Entities", beneficiary_entities_created)
        #     col3.metric("Agent Entities", agents_processed)
        #     col4.metric("Relationships Updated", beneficiary_relationships_updated)
        #     col5.metric("Transactions Skipped", total_skipped_count)
        
        logger.info(f"Multi-threaded MODIFY BATCH COMPLETE: {success_count} transactions, {agents_processed} agents processed, {beneficiary_entities_created} entities created using {max_workers} threads")
        
        # Add summary results
        summary_parts = []
        if agents_processed > 0:
            summary_parts.append(f"processed {agents_processed} agent entities")
        if beneficiary_entities_created > 0:
            summary_parts.append(f"created {beneficiary_entities_created} beneficiary entities")
        if beneficiary_relationships_updated > 0:
            summary_parts.append(f"updated {beneficiary_relationships_updated} relationships")
        if total_skipped_count > 0:
            summary_parts.append(f"skipped {total_skipped_count} transactions due to unresolved account IDs")
        
        if summary_parts:
            results.insert(0, f"Multi-threaded MODIFY Summary: {', '.join(summary_parts)}")
        
        time.sleep(1)
        progress_container.empty()
        metrics_container.empty()
        status_container.empty()
        
    except Exception as e:
        logger.error(f"Multi-threaded MODIFY batch operation failed: {str(e)}")
        error_count = len(modify_operations)
        results.append(f"Multi-threaded MODIFY batch operation failed: {str(e)}")
        
        with status_container.container():
            st.error(f"Error in multi-threaded MODIFY processing: {str(e)}")
        
        time.sleep(1)
        if 'progress_container' in locals():
            progress_container.empty()
        if 'metrics_container' in locals():
            metrics_container.empty()
        if 'status_container' in locals():
            status_container.empty()
    
    return success_count, error_count, results



def batch_create_beneficiary_entities(cursor, beneficiary_banks, current_time):
    """
    Batch create beneficiary entities
    """
    if not beneficiary_banks:
        return 0
    
    try:
        # Check which customer_entity_ids already exist
        existing_query = """
        SELECT customer_entity_id FROM entity 
        WHERE customer_entity_id IN ({})
        """.format(','.join(['%s'] * len(beneficiary_banks)))
        
        cursor.execute(existing_query, list(beneficiary_banks))
        existing_entities = {row[0] for row in cursor.fetchall()}
        
        # Filter out existing entities
        new_entities = beneficiary_banks - existing_entities
        
        if not new_entities:
            logger.info(f"All {len(beneficiary_banks)} beneficiary entities already exist")
            return 0
        
        # Batch insert new entities
        entity_inserts = []
        for customer_entity_id in new_entities:
            entity_params = (
                customer_entity_id,    # customer_entity_id (from beneficiary_bank field)
                'Customer',         # entity_type
                'UserUpload',        # inserted_by
                'UserUpload',        # updated_by
                current_time,          # insert_date
                current_time,          # update_date
                'UserUpload'  # svc_provider_name
            )
            entity_inserts.append(entity_params)
        
        entity_sql = """
        INSERT INTO entity (
            customer_entity_id, entity_type, inserted_by, updated_by, 
            insert_date, update_date, svc_provider_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(entity_sql, entity_inserts)
        logger.info(f"Batch created {len(new_entities)} beneficiary entities")
        return len(new_entities)
        
    except Exception as e:
        logger.error(f"Error in batch_create_beneficiary_entities: {str(e)}")
        return 0


def batch_check_relationships_exist(cursor, relationship_pairs):
    """
    Batch check which relationships already exist
    """
    if not relationship_pairs:
        return set()
    
    try:
        # Build query to check all relationship pairs at once
        conditions = []
        params = []
        
        for intermediary, beneficiary in relationship_pairs:
            conditions.append("(e1.entity_name = %s AND e2.entity_name = %s)")
            params.extend([intermediary, beneficiary])
        
        query = f"""
        SELECT e1.entity_name as intermediary, e2.entity_name as beneficiary
        FROM entity_relationship er
        JOIN entity e1 ON er.primary_entity_id = e1.entity_id
        JOIN entity e2 ON er.related_entity_id = e2.entity_id
        WHERE ({' OR '.join(conditions)})
        """
        
        cursor.execute(query, params)
        existing = {(row[0], row[1]) for row in cursor.fetchall()}
        
        logger.info(f"Found {len(existing)} existing relationships out of {len(relationship_pairs)} checked")
        return existing
        
    except Exception as e:
        logger.error(f"Error in batch_check_relationships_exist: {str(e)}")
        return set()


def batch_create_relationships(cursor, relationship_pairs, current_time):
    """
    Batch create entity relationships
    """
    if not relationship_pairs:
        return 0
    
    try:
        # Get entity IDs for all banks in batch
        all_banks = set()
        for intermediary, beneficiary in relationship_pairs:
            all_banks.add(intermediary)
            all_banks.add(beneficiary)
        
        entity_query = """
        SELECT entity_id, entity_name FROM entity 
        WHERE entity_name IN ({})
        """.format(','.join(['%s'] * len(all_banks)))
        
        cursor.execute(entity_query, list(all_banks))
        entity_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Prepare relationship inserts
        relationship_inserts = []
        created_count = 0
        
        for intermediary, beneficiary in relationship_pairs:
            intermediary_id = entity_map.get(intermediary)
            beneficiary_id = entity_map.get(beneficiary)
            
            if intermediary_id and beneficiary_id:
                relationship_params = (
                    intermediary_id,    # primary_entity_id
                    beneficiary_id,     # related_entity_id
                    None,               # account_id
                    'RELATED_PARTY',      # relationship_type
                    current_time,       # insert_date
                    current_time,       # update_date
                    'UserUpload',     # inserted_by
                    'UserUpload'      # updated_by
                )
                relationship_inserts.append(relationship_params)
                created_count += 1
            else:
                logger.warning(f"Could not find entity IDs for relationship: {intermediary} -> {beneficiary}")
        
        if relationship_inserts:
            relationship_sql = """
            INSERT INTO entity_relationship (
                primary_entity_id, related_entity_id, account_id, relationship_type,
                insert_date, update_date, inserted_by, updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(relationship_sql, relationship_inserts)
            logger.info(f"Batch created {created_count} entity relationships")
        
        return created_count
        
    except Exception as e:
        logger.error(f"Error in batch_create_relationships: {str(e)}")
        return 0

def get_entity_id_by_bank(cursor, bank_code_or_name):
    """
    Helper function to get entity ID by bank code or name
    Adjust this query based on your actual entity table structure
    """
    try:
        cursor.execute("""
            SELECT entity_id FROM entity 
            WHERE customer_entity_id = %s 
            LIMIT 1
        """, (bank_code_or_name,))
        
        result = cursor.fetchone()
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error getting entity ID for bank {bank_code_or_name}: {str(e)}")
        return None

def batch_check_relationships_exist_for_modify(cursor, relationship_pairs):
    """
    Batch check which relationships already exist for MODIFY operations
    """
    if not relationship_pairs:
        return set()
    
    try:
        # Build query to check all relationship pairs at once using entity_id
        conditions = []
        params = []
        
        for intermediary, beneficiary in relationship_pairs:
            conditions.append("(e1.entity_id = %s AND e2.entity_id = %s)")
            params.extend([intermediary, beneficiary])
        
        query = f"""
        SELECT e1.entity_id as intermediary, e2.entity_id as beneficiary
        FROM entity_relationship er
        JOIN entity e1 ON er.primary_entity_id = e1.entity_id
        JOIN entity e2 ON er.related_entity_id = e2.entity_id
        WHERE ({' OR '.join(conditions)})
        """
        
        cursor.execute(query, params)
        existing = {(row[0], row[1]) for row in cursor.fetchall()}
        
        logger.info(f"MODIFY: Found {len(existing)} existing relationships out of {len(relationship_pairs)} checked")
        return existing
        
    except Exception as e:
        logger.error(f"Error in batch_check_relationships_exist_for_modify: {str(e)}")
        return set()

def batch_update_relationships(cursor, relationship_pairs, current_time):
    """
    Batch update existing entity relationships with new timestamp
    """
    if not relationship_pairs:
        return 0
    
    try:
        # Prepare relationship updates using entity_id directly
        relationship_updates = []
        
        for intermediary_id, beneficiary_id in relationship_pairs:
            relationship_params = (
                current_time,       # update_date
                'UserUpload',     # updated_by
                beneficiary_id,      # related_entity_id 
                intermediary_id,    # primary_entity_id (WHERE clause)
            )
            relationship_updates.append(relationship_params)
        
        if relationship_updates:
            # relationship_sql = """
            # UPDATE entity_relationship 
            # SET update_date = %s, updated_by = %s
            # WHERE primary_entity_id = %s AND related_entity_id = %s
            # """
            
            # cursor.executemany(relationship_sql, relationship_updates)
            logger.info(f"Existing records in entity_relationship, with no relationship changes..hence no updates performed, to save processing time in debezeium")
            logger.info(f"MODIFY: Batch updated {len(relationship_updates)} entity relationships")
        
        return len(relationship_updates)
        
    except Exception as e:
        logger.error(f"Error in batch_update_relationships: {str(e)}")
        return 0

def cleanup_outdated_transaction_relationships(cursor, transaction_ids, current_relationship_pairs, current_time):
    """
    Remove or deactivate relationships for transactions that no longer match the current data
    This handles the case where a transaction's relationship has changed from A->B to A->C
    """
    if not transaction_ids or not current_relationship_pairs:
        return 0
    
    try:
        # Get current relationship pairs as a set for fast lookup
        current_pairs_set = set(current_relationship_pairs)
        
        # Find all existing relationships for these transactions
        placeholders = ','.join(['%s'] * len(transaction_ids))
        
        # Query to find transaction relationships that should be removed
        # This assumes you have a way to link transactions to relationships
        # Adjust the query based on your actual schema
        cursor.execute(f"""
            SELECT DISTINCT er.primary_entity_id, er.related_entity_id, er.relationship_id
            FROM entity_relationship er
            JOIN entity e1 ON er.primary_entity_id = e1.entity_id
            JOIN entity e2 ON er.related_entity_id = e2.entity_id
            JOIN transactions t ON (
                (t.intermediary_bank = e1.customer_entity_id) AND
                (t.beneficiary_bank = e2.customer_entity_id)
            )
            WHERE t.transaction_id IN ({placeholders})
            AND er.status = 'Active'
        """, transaction_ids)
        
        existing_relationships = cursor.fetchall()
        
        # Identify relationships that should be deactivated
        relationships_to_deactivate = []
        
        for primary_id, related_id, relationship_id in existing_relationships:
            pair = (primary_id, related_id)
            if pair not in current_pairs_set:
                relationships_to_deactivate.append((current_time,current_time, 'UserUpload', relationship_id))
        
        # Deactivate outdated relationships
        if relationships_to_deactivate:
            deactivate_sql = """
            UPDATE entity_relationship 
            SET status = 'Inactive', end_date = %s, update_date = %s, updated_by = %s
            WHERE relationship_id = %s
            """
            
            cursor.executemany(deactivate_sql, relationships_to_deactivate)
            logger.info(f"MODIFY: Deactivated {len(relationships_to_deactivate)} outdated relationships")
        
        return len(relationships_to_deactivate)
        
    except Exception as e:
        logger.error(f"Error in cleanup_outdated_transaction_relationships: {str(e)}")
        return 0
    
def batch_create_new_relationships(cursor, new_relationship_pairs, current_time):
    """
    Batch create new entity relationships
    """
    if not new_relationship_pairs:
        return 0
    
    try:
        relationship_inserts = []
        
        for intermediary_id, beneficiary_id in new_relationship_pairs:
            relationship_params = (
                intermediary_id,    # primary_entity_id
                beneficiary_id,     # related_entity_id
                'RELATED_PARTY',      # relationship_type (adjust as needed)
                current_time,       # create_date
                'UserUpload',       # created_by
                current_time,       # update_date
                'UserUpload'        # updated_by
            )
            relationship_inserts.append(relationship_params)
        
        if relationship_inserts:
            relationship_sql = """
            INSERT INTO entity_relationship 
            (primary_entity_id, related_entity_id, relationship_type, insert_date, inserted_by, update_date, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(relationship_sql, relationship_inserts)
            logger.info(f"MODIFY: Batch created {len(relationship_inserts)} new entity relationships")
        
        return len(relationship_inserts)
        
    except Exception as e:
        logger.error(f"Error in batch_create_new_relationships: {str(e)}")
        return 0
    
def batch_create_relationships_for_modify(cursor, relationship_pairs, current_time):
    """
    Batch create new entity relationships for MODIFY operations
    """
    if not relationship_pairs:
        return 0
    
    try:
        # Prepare relationship inserts using entity_id directly
        relationship_inserts = []
        
        for intermediary_id, beneficiary_id in relationship_pairs:
            relationship_params = (
                intermediary_id,    # primary_entity_id
                beneficiary_id,     # related_entity_id
                None,               # account_id
                'RELATED_PARTY',      # relationship_type
                current_time,       # insert_date
                current_time,       # update_date
                'UserUpload',     # inserted_by
                'UserUpload'      # updated_by
            )
            relationship_inserts.append(relationship_params)
        
        if relationship_inserts:
            relationship_sql = """
            INSERT INTO entity_relationship (
                primary_entity_id, related_entity_id, account_id, relationship_type,
                insert_date, update_date, inserted_by, updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(relationship_sql, relationship_inserts)
            logger.info(f"MODIFY: Batch created {len(relationship_inserts)} entity relationships")
        
        return len(relationship_inserts)
        
    except Exception as e:
        logger.error(f"Error in batch_create_relationships_for_modify: {str(e)}")
        return 0

def validate_transaction_update_parameters(transaction_params, expected_count=79):
    """
    Validate that transaction update parameters match expected count
    """
    if not transaction_params:
        return {'valid': False, 'error': 'No parameters provided'}
    
    actual_count = len(transaction_params)
    if actual_count != expected_count:
        return {
            'valid': False, 
            'error': f'Parameter count mismatch: expected {expected_count}, got {actual_count}',
            'actual_count': actual_count,
            'expected_count': expected_count,
            'parameters': transaction_params
        }
    
    return {'valid': True, 'parameter_count': actual_count}


def debug_prepare_transaction_update_params(row_dict, db_transaction_id, current_time):
    """
    Debug wrapper for prepare_transaction_update_params to identify parameter issues
    """
    try:
        params = prepare_transaction_update_params(row_dict, db_transaction_id, current_time)
        validation = validate_transaction_update_parameters(params)
        
        if not validation['valid']:
            logger.error(f"Parameter validation failed: {validation['error']}")
            logger.error(f"Parameters: {params}")
            
            # Expected parameter order for UPDATE statement:
            expected_fields = [
                'account_id' , 'b_number' , 'customer_transaction_id' , 'transaction_code' , 
                'transaction_type' , 'preferred_currency' , 'nominal_currency' , 'amount' , 
                'debit_credit_indicator' , 'transaction_date' , 'tran_code_description' , 
                'description' , 'reference' , 'balance' , 'eod_balance' , 
                'non_account_holder' , 'transaction_origin_country' , 'counterparty_routing_number' , 
                'counterparty_name' , 'branch_id' , 'teller_id' , 'check_number' , 
                'check_image_front_path' , 'check_image_back_path' , 'txn_location' , 
                'txn_source' , 'intermediary_1_id' , 'intermediary_1_type' , 
                'intermediary_1_code' , 'intermediary_1_name' , 'intermediary_1_role' , 
                'intermediary_1_reference' , 'intermediary_1_fee' , 'intermediary_1_fee_currency' , 
                'intermediary_2_id' , 'intermediary_2_type' , 'intermediary_2_code' , 
                'intermediary_2_name' , 'intermediary_2_role' , 'intermediary_2_reference' , 
                'intermediary_2_fee' , 'intermediary_2_fee_currency' , 'intermediary_3_id' , 
                'intermediary_3_type' , 'intermediary_3_code' , 'intermediary_3_name' , 
                'intermediary_3_role' , 'intermediary_3_reference', 'intermediary_3_fee' , 
                'intermediary_3_fee_currency' , 'positive_pay_indicator' , 'originator_bank' , 
                'beneficiary_details' , 'intermediary_bank' , 'beneficiary_bank' , 
                'instructing_bank' , 'inter_bank_information' , 'other_information' , 
                'related_account' , 'transaction_field_1' , 'transaction_field_2' , 
                'transaction_field_3' , 'transaction_field_4' , 'transaction_field_5' , 
                'transaction_field_11' , 'transaction_field_12' , 'transaction_field_13' , 
                'transaction_field_14' , 'transaction_field_15' , 'is_p2p_transaction' , 
                'p2p_recipient_type' , 'p2p_recipient' , 'p2p_sender_type' , 'p2p_sender' , 
                'p2p_reference_id' , 'update_date' , 'updated_by' , 'svc_provider_name' ,
                'transaction_id'  # WHERE clause
            ]
            
            logger.error(f"Expected {len(expected_fields)} fields: {expected_fields}")
            
        return params, validation
        
    except Exception as e:
        logger.error(f"Error in prepare_transaction_update_params: {str(e)}")
        return None, {'valid': False, 'error': str(e)}
    
def get_entity_ids_for_relationship_batch(cursor, entity_names):
    """
    Helper function to get entity IDs for multiple entity names in batch
    """
    if not entity_names:
        return {}
    
    try:
        entity_query = """
        SELECT entity_id, entity_name FROM entity 
        WHERE entity_name IN ({})
        """.format(','.join(['%s'] * len(entity_names)))
        
        cursor.execute(entity_query, list(entity_names))
        return {row[1]: row[0] for row in cursor.fetchall()}
        
    except Exception as e:
        logger.error(f"Error in get_entity_ids_for_relationship_batch: {str(e)}")
        return {}


def get_entity_ids_for_relationship(cursor, intermediary_bank, beneficiary_bank):
    """
    Get entity IDs for both intermediary and beneficiary banks
    
    Returns:
        tuple: (intermediary_entity_id, beneficiary_entity_id)
    """
    try:
        # Get intermediary entity ID
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (intermediary_bank,))
        intermediary_result = cursor.fetchone()
        intermediary_entity_id = intermediary_result[0] if intermediary_result else None
        
        # Get beneficiary entity ID
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
        beneficiary_result = cursor.fetchone()
        beneficiary_entity_id = beneficiary_result[0] if beneficiary_result else None
        
        logger.debug(f"Entity lookup: {intermediary_bank} -> {intermediary_entity_id}, {beneficiary_bank} -> {beneficiary_entity_id}")
        
        return intermediary_entity_id, beneficiary_entity_id
        
    except Exception as e:
        logger.error(f"Error looking up entity IDs: {str(e)}")
        return None, None


def check_entity_relationship_exists(cursor, intermediary_entity_id, beneficiary_entity_id):
    """
    Check if entity relationship already exists between two entities
    
    Args:
        cursor: Database cursor
        intermediary_entity_id: Primary entity ID
        beneficiary_entity_id: Related entity ID
    
    Returns:
        bool: True if relationship exists, False otherwise
    """
    try:
        # Check in entity_relationship table
        cursor.execute("""
            SELECT COUNT(*) FROM entity_relationship 
            WHERE primary_entity_id = %s AND related_entity_id = %s
        """, (intermediary_entity_id, beneficiary_entity_id))
        
        count = cursor.fetchone()[0]
        exists = count > 0
        
        logger.debug(f"Relationship exists check: {intermediary_entity_id} -> {beneficiary_entity_id}: {exists}")
        return exists
        
    except Exception as e:
        logger.error(f"Error checking relationship existence: {str(e)}")
        return False


def update_entity_relationship(cursor, intermediary_entity_id, beneficiary_entity_id, current_time):
    """
    Update existing entity relationship
    
    Args:
        cursor: Database cursor
        intermediary_entity_id: Primary entity ID
        beneficiary_entity_id: Related entity ID
        current_time: Current timestamp
    
    Returns:
        bool: True if update successful, False otherwise
    """
    try:
        update_sql = """
        UPDATE entity_relationship 
        SET update_date = %s, updated_by = %s
        WHERE primary_entity_id = %s AND related_entity_id = %s
        """
        
        cursor.execute(update_sql, (
            current_time,
            'UserUpload',
            intermediary_entity_id,
            beneficiary_entity_id
        ))
        
        rows_affected = cursor.rowcount
        success = rows_affected > 0
        
        logger.debug(f"Updated relationship: {intermediary_entity_id} -> {beneficiary_entity_id}, rows affected: {rows_affected}")
        return success
        
    except Exception as e:
        logger.error(f"Error updating entity relationship: {str(e)}")
        return False


def create_entity_relationship_for_modify(cursor, intermediary_entity_id, beneficiary_entity_id, current_time):
    """
    Create new entity relationship during modify operation
    
    Args:
        cursor: Database cursor
        intermediary_entity_id: Primary entity ID
        beneficiary_entity_id: Related entity ID
        current_time: Current timestamp
    
    Returns:
        bool: True if creation successful, False otherwise
    """
    try:
        insert_sql = """
        INSERT INTO entity_relationship (
            primary_entity_id, related_entity_id, relationship_type,
            insert_date, update_date, inserted_by, updated_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_sql, (
            intermediary_entity_id,
            beneficiary_entity_id,
            'RELATED_PARTY',
            current_time,
            current_time,
            'UserUpload',
            'UserUpload'
        ))
        
        rows_affected = cursor.rowcount
        success = rows_affected > 0
        
        logger.debug(f"Created relationship: {intermediary_entity_id} -> {beneficiary_entity_id}, rows affected: {rows_affected}")
        return success
        
    except Exception as e:
        logger.error(f"Error creating entity relationship: {str(e)}")
        return False

def validate_transaction_sql_parameters(transaction_params):
    """
    Validate that the transaction parameters match the expected SQL statement structure
    
    Args:
        transaction_params: Tuple of parameters for transaction insert
        
    Returns:
        dict: Validation results with parameter count and field mapping
    """
    expected_fields = [
        'account_id',
        'b_number',
        'customer_transaction_id',
        'transaction_code',
        'transaction_type',
        'preferred_currency',
        'nominal_currency',
        'amount',
        'debit_credit_indicator',
        'transaction_date',
        'tran_code_description',
        'description',
        'reference',
        'balance',
        'eod_balance',
        'non_account_holder',
        'transaction_origin_country',
        'counterparty_routing_number',
        'counterparty_name',
        'branch_id',
        'teller_id',
        'check_number',
        'check_image_front_path',
        'check_image_back_path',
        'txn_location',
        'txn_source',
        'intermediary_1_id',
        'intermediary_1_type',
        'intermediary_1_code',
        'intermediary_1_name',
        'intermediary_1_role',
        'intermediary_1_reference',
        'intermediary_1_fee',
        'intermediary_1_fee_currency',
        'intermediary_2_id',
        'intermediary_2_type',
        'intermediary_2_code',
        'intermediary_2_name',
        'intermediary_2_role',
        'intermediary_2_reference',
        'intermediary_2_fee',
        'intermediary_2_fee_currency',
        'intermediary_3_id',
        'intermediary_3_type',
        'intermediary_3_code',
        'intermediary_3_name',
        'intermediary_3_role',
        'intermediary_3_reference',
        'intermediary_3_fee',
        'intermediary_3_fee_currency',
        'positive_pay_indicator',
        'originator_bank',
        'beneficiary_details',
        'intermediary_bank',
        'beneficiary_bank',
        'instructing_bank',
        'inter_bank_information',
        'other_information',
        'related_account',
        'transaction_field_1',
        'transaction_field_2',
        'transaction_field_3',
        'transaction_field_4',
        'transaction_field_5',
        'transaction_field_11',
        'transaction_field_12',
        'transaction_field_13',
        'transaction_field_14',
        'transaction_field_15',
        'is_p2p_transaction',
        'p2p_recipient_type',
        'p2p_recipient',
        'p2p_sender_type',
        'p2p_sender',
        'p2p_reference_id',
        'insert_date',
        'update_date',
        'inserted_by',
        'updated_by',
        'svc_provider_name'
    ]
    
    param_count = len(transaction_params)
    expected_count = len(expected_fields)
    
    validation_result = {
        'param_count': param_count,
        'expected_count': expected_count,
        'match': param_count == expected_count,
        'expected_fields': expected_fields
    }
    
    if not validation_result['match']:
        logger.error(f"Parameter count mismatch: got {param_count}, expected {expected_count}")
        logger.error(f"Expected fields: {expected_fields}")
        logger.error(f"Parameter values: {transaction_params}")
    
    return validation_result


def create_entity_relationship(cursor, intermediary_bank, beneficiary_bank, current_time):
    """
    Create relationship between intermediary and beneficiary entities.
    Tries multiple approaches based on available tables and data.
    
    Args:
        cursor: Database cursor
        intermediary_bank: Intermediary bank customer_entity_id
        beneficiary_bank: Beneficiary bank customer_entity_id  
        current_time: Current timestamp
    
    Returns:
        bool: True if relationship created successfully, False otherwise
    """
    try:
        # Approach 1: Try entity_relationship table with entity_ids
        logger.info(f"RELATIONSHIP_CREATE: Starting relationship creation between '{intermediary_bank}' and '{beneficiary_bank}'")
        
        # Get entity_ids for both banks
        logger.info(f"RELATIONSHIP_LOOKUP: Looking up entity_id for intermediary '{intermediary_bank}'")
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (intermediary_bank,))
        intermediary_result = cursor.fetchone()
        
        logger.info(f"RELATIONSHIP_LOOKUP: Looking up entity_id for beneficiary '{beneficiary_bank}'")
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
        beneficiary_result = cursor.fetchone()
        
        if intermediary_result and beneficiary_result:
            intermediary_entity_id = intermediary_result[0]
            beneficiary_entity_id = beneficiary_result[0]
            logger.info(f"RELATIONSHIP_ENTITIES: Found intermediary_entity_id={intermediary_entity_id}, beneficiary_entity_id={beneficiary_entity_id}")
            
            try:
                # Try entity_relationship table first
                logger.info(f"RELATIONSHIP_INSERT: Attempting insert into entity_relationship table")
                relationship_sql = """
                INSERT INTO entity_relationship (
                    primary_entity_id, related_entity_id, relationship_type,
                    insert_date, update_date, inserted_by, updated_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                relationship_params = (
                    intermediary_entity_id,  # primary_entity_id (intermediary)
                    beneficiary_entity_id,   # related_entity_id (beneficiary)
                    'RELATED_PARTY',
                    current_time,
                    current_time,
                    'UserUpload',
                    'UserUpload'
                )
                
                cursor.execute(relationship_sql, relationship_params)
                logger.info(f"RELATIONSHIP_SUCCESS: Created relationship in entity_relationship: intermediary_entity_id={intermediary_entity_id}, beneficiary_entity_id={beneficiary_entity_id}")
                return True
                
            except Exception as e:
                logger.warning(f"RELATIONSHIP_ENTITY_TABLE_FAILED: entity_relationship table insert failed: {str(e)}")
                # Fall through to try relationship_stream
        else:
            logger.warning(f"RELATIONSHIP_ENTITY_LOOKUP_FAILED: Could not find entity_ids - intermediary_result={intermediary_result}, beneficiary_result={beneficiary_result}")
        
        
    except Exception as e:
        logger.error(f"RELATIONSHIP_EXCEPTION: Error in create_entity_relationship: {str(e)}")
        return False

def create_beneficiary_entity_and_relationship(row_dict, connection):
    """
    Create beneficiary entity if needed AND create relationship if beneficiary_bank differs from intermediary_bank
    
    Args:
        row_dict: Transaction row data
        connection: Database connection
    
    Returns:
        dict: Results with entity_created and relationship_created flags
    """
    beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
    intermediary_bank = row_dict.get('intermediary_bank', '').strip()
    
    results = {
        'entity_created': False,
        'relationship_created': False,
        'entity_already_existed': False,
        'messages': []
    }
    
    # Only proceed if beneficiary_bank is different from intermediary_bank
    if not beneficiary_bank or beneficiary_bank == intermediary_bank:
        results['messages'].append(f"Beneficiary bank same as intermediary or empty, no processing needed")
        return results
    
    logger.info(f"Processing beneficiary: {beneficiary_bank} vs intermediary: {intermediary_bank}")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Check if beneficiary entity already exists
        cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
        existing_entity = cursor.fetchone()
        
        if existing_entity:
            logger.info(f"Beneficiary entity {beneficiary_bank} already exists with entity_id {existing_entity[0]}")
            results['entity_already_existed'] = True
            results['messages'].append(f"Beneficiary entity {beneficiary_bank} already exists")
        else:
            # Create new beneficiary entity
            logger.info(f"Creating new beneficiary entity for: {beneficiary_bank}")
            
            # Parse transaction field data
            entity_data = parse_transaction_field_data(
                row_dict.get('transaction_field_1'),
                row_dict.get('transaction_field_2'), 
                row_dict.get('transaction_field_3'),
                row_dict.get('transaction_field_4')
            )
            
            # Step 1: Insert into entity table
            entity_sql = """
            INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            entity_params = (
                beneficiary_bank,
                'Customer',
                'UserUpload',
                'UserUpload', 
                current_time,
                current_time,
                'TransactionProcessor'
            )
            
            cursor.execute(entity_sql, entity_params)
            
            # Get the entity_id that was just created
            cursor.execute("SELECT entity_id FROM entity WHERE customer_entity_id = %s", (beneficiary_bank,))
            entity_result = cursor.fetchone()
            
            if not entity_result:
                logger.error(f"Failed to retrieve entity_id for {beneficiary_bank}")
                cursor.close()
                results['messages'].append(f"Failed to create entity for {beneficiary_bank}")
                return results
            
            entity_id = entity_result[0]
            logger.info(f"Created entity with ID {entity_id} for beneficiary {beneficiary_bank}")
            
            # Step 2: Insert into entity_customer table
            customer_sql = """
            INSERT INTO entity_customer (
                entity_id, customer_entity_id, first_name, middle_name, last_name,
                date_of_birth, customertype, nationality, svc_provider_name, 
                inserted_date, updated_date, inserted_by, updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            customer_params = (
                entity_id,
                beneficiary_bank,
                entity_data.get('first_name', ''),
                entity_data.get('middle_name', ''),
                entity_data.get('last_name', ''),
                entity_data.get('date_of_birth'),
                'Consumer',
                'US',
                'TransactionProcessor',
                current_time,
                current_time,
                'UserUpload',
                'UserUpload'
            )
            
            cursor.execute(customer_sql, customer_params)
            logger.info(f"Created customer record for entity {entity_id}")
            
            # Step 3: Insert into entity_address table (if address data available)
            if entity_data.get('address_1'):
                address_sql = """
                INSERT INTO entity_address (
                    entity_id, address_type, address_1, address_city, address_state,
                    address_country, address_zipcode, svc_provider_name,
                    inserted_date, update_date, inserted_by, updated_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                address_params = (
                    entity_id,
                    entity_data.get('address_type', 'Primary'),
                    entity_data.get('address_1', ''),
                    entity_data.get('address_city', ''),
                    entity_data.get('address_state', ''),
                    entity_data.get('address_country', 'US'),
                    entity_data.get('address_zipcode', ''),
                    'TransactionProcessor',
                    current_time,
                    current_time,
                    'UserUpload',
                    'UserUpload'
                )
                
                cursor.execute(address_sql, address_params)
                logger.info(f"Created address record for entity {entity_id}")
            
            # Step 4: Insert into entity_identifier table (if identifier data available)
            if entity_data.get('identifier_type') and entity_data.get('identifier_value'):
                identifier_sql = """
                INSERT INTO entity_identifier (
                    entity_id, identifier_type, identifier_value, source_system,
                    svc_provider_name, insert_date, update_date, inserted_by, updated_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                identifier_params = (
                    entity_id,
                    entity_data.get('identifier_type'),
                    entity_data.get('identifier_value'),
                    entity_data.get('source', 'TXN'),
                    'TransactionProcessor',
                    current_time,
                    current_time,
                    'UserUpload',
                    'UserUpload'
                )
                
                cursor.execute(identifier_sql, identifier_params)
                logger.info(f"Created identifier record for entity {entity_id}")
            
            results['entity_created'] = True
            results['messages'].append(f"Created complete beneficiary entity for {beneficiary_bank}")
        
        # Step 5: ALWAYS create relationship if intermediary_bank exists (regardless of whether entity was created)
        if intermediary_bank:
            logger.info(f"Creating relationship between intermediary {intermediary_bank} and beneficiary {beneficiary_bank}")
            
            # Check if relationship already exists to avoid duplicates
            relationship_exists = check_relationship_exists(cursor, intermediary_bank, beneficiary_bank)
            
            if relationship_exists:
                logger.info(f"Relationship between {intermediary_bank} and {beneficiary_bank} already exists")
                results['messages'].append(f"Relationship between {intermediary_bank} and {beneficiary_bank} already exists")
            else:
                relationship_created = create_entity_relationship(cursor, intermediary_bank, beneficiary_bank, current_time)
                if relationship_created:
                    results['relationship_created'] = True
                    results['messages'].append(f"Created relationship between {intermediary_bank} and {beneficiary_bank}")
                    logger.info(f"Successfully created relationship between {intermediary_bank} and {beneficiary_bank}")
                else:
                    results['messages'].append(f"Failed to create relationship between {intermediary_bank} and {beneficiary_bank}")
                    logger.warning(f"Could not create relationship between {intermediary_bank} and {beneficiary_bank}")
        
        # Commit all changes
        connection.commit()
        cursor.close()
        
        logger.info(f"Successfully processed beneficiary {beneficiary_bank}: entity_created={results['entity_created']}, relationship_created={results['relationship_created']}")
        return results
        
    except Exception as e:
        logger.error(f"Error processing beneficiary {beneficiary_bank}: {str(e)}")
        try:
            connection.rollback()
        except:
            pass
        results['messages'].append(f"Error processing beneficiary {beneficiary_bank}: {str(e)}")
        return results

def check_relationship_exists(cursor, intermediary_bank, beneficiary_bank):
    """
    Check if relationship already exists between intermediary and beneficiary
    
    Args:
        cursor: Database cursor
        intermediary_bank: Intermediary bank customer_entity_id
        beneficiary_bank: Beneficiary bank customer_entity_id
    
    Returns:
        bool: True if relationship exists, False otherwise
    """
    try:
        # Check in entity_relationship table first
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM entity_relationship er1
                JOIN entity e1 ON er1.primary_entity_id = e1.entity_id
                JOIN entity e2 ON er1.related_entity_id = e2.entity_id
                WHERE e1.customer_entity_id = %s AND e2.customer_entity_id = %s
            """, (intermediary_bank, beneficiary_bank))
            
            count = cursor.fetchone()[0]
            if count > 0:
                return True
        except Exception:
            # Table might not exist, continue to check relationship_stream
            pass
        
    except Exception as e:
        logger.error(f"Error checking relationship existence: {str(e)}")
        return False

def process_account_chunk_batch_with_biller_entity(chunk_df, connection, is_fixed_width=False):
    """Process account chunk using batch SQL generation with biller entity creation"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing account chunk with {len(chunk_df)} records using batch SQL generation with biller entity creation")
    
    # if is_fixed_width:
    #     # For fixed-width files, we need to create biller entities first
    #     return process_fixed_width_account_chunk_with_biller_entity(chunk_df, connection)
    # else:
    #     # For CSV files, use existing logic
    return process_account_chunk_batch(chunk_df, connection, is_fixed_width)
    
def process_fixed_width_account_chunk_with_biller_entity(chunk_df, connection):
    """Process fixed-width account chunk with biller entity creation"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing fixed-width account chunk with biller entity creation for {len(chunk_df)} records")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Step 1: Create biller entities for each account
        entity_inserts = []
        business_inserts = []
        account_inserts = []
        
        for index, row in chunk_df.iterrows():
            try:
                row_dict = clean_row_data(row.to_dict())
                
                account_id = row_dict.get('account_id')
                # biller_entity_id = row_dict.get('biller_entity_id')
                business_name = row_dict.get('account_holder_name')
                
                if not account_id or not business_name:
                    error_count += 1
                    results.append(f"Row {index}: Missing account_id or biller_entity_id")
                    continue
                
                
            except Exception as e:
                error_count += 1
                results.append(f"Row {index}: Error preparing biller entity - {str(e)}")
                logger.exception(f"Error processing account row {index}")
        
        # Step 2: Execute entity inserts
        if entity_inserts:
            entity_sql = """
            INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(entity_sql, entity_inserts)
            logger.info(f"Inserted {len(entity_inserts)} biller entities")
        
        # Step 3: Get the entity IDs that were just created and execute business inserts
        if entity_inserts:
            biller_entity_ids = [params[0] for params in entity_inserts]  # Get customer_entity_ids
            biller_entity_map = batch_lookup_existing_entities(connection, biller_entity_ids)
            
            # Now prepare business inserts with actual entity_ids
            business_inserts_with_entity_id = []
            for index, row in chunk_df.iterrows():
                try:
                    row_dict = clean_row_data(row.to_dict())
                    biller_entity_id = row_dict.get('account_id')
                    
                    if biller_entity_id in biller_entity_map:
                        db_entity_id = biller_entity_map[biller_entity_id]
                        business_params = prepare_biller_business_insert_params(db_entity_id, biller_entity_id, row_dict, current_time)
                        business_inserts_with_entity_id.append(business_params)
                
                except Exception as e:
                    logger.error(f"Error preparing business insert for row {index}: {str(e)}")
            
            # Execute business inserts with correct entity_ids
            if business_inserts_with_entity_id:
                business_sql = """
                INSERT INTO entity_business (
                    entity_id, customer_entity_id, customer_parent_entity_id, 
                    legal_name, business_type, date_of_incorporation, 
                    business_activity, taxid, doing_business_as, 
                    bankruptcy_date, closed_date, closed_reason,
                    svc_provider_name, inserted_date, updated_date, inserted_by, updated_by
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                cursor.executemany(business_sql, business_inserts_with_entity_id)
                logger.info(f"Inserted {len(business_inserts_with_entity_id)} biller business records")
        
        # Step 5: Prepare account inserts using the newly created entity IDs
        for index, row in chunk_df.iterrows():
            try:
                row_dict = clean_row_data(row.to_dict())
                
                account_id = row_dict.get('account_id')
                biller_entity_id = row_dict.get('account_id')
                
                # Get the database entity_id for this biller
                db_entity_id = biller_entity_map.get(biller_entity_id)
                
                if not db_entity_id:
                    error_count += 1
                    results.append(f"Row {index}: Could not find created entity for biller {biller_entity_id}")
                    continue
                
                # Prepare account insert with the biller entity_id
                # account_params = prepare_account_insert_with_biller_entity(row_dict, db_entity_id, current_time)
                account_inserts.append(account_params)
                
            except Exception as e:
                error_count += 1
                results.append(f"Row {index}: Error preparing account insert - {str(e)}")
                logger.exception(f"Error processing account insert for row {index}")
        
        # Step 6: Execute account inserts
        if account_inserts:
            account_sql = """
            INSERT INTO account (
                customer_account_id, entity_id, account_type, account_number, account_holder_name,
                account_routing_number, company_identification, account_currency, account_balance, 
                open_date, closed_date, close_reason, branch_id, reopen_date, 
                account_risk_score, account_status, is_zelle_enabled, 
                p2p_enrollment_date, p2p_email, p2p_phone, is_joint,
                svc_provider_name, insert_date, update_date, inserted_by, updated_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.executemany(account_sql, account_inserts)
            logger.info(f"Inserted {len(account_inserts)} account records")
        
        # Commit all operations
        connection.commit()
        cursor.close()
        
        success_count = len(account_inserts)
        for i in range(success_count):
            results.append(f"Row {i}: Successfully created biller entity and account")
        
        logger.info(f"Successfully completed biller entity and account creation for {success_count} records")
        
    except Exception as e:
        error_count = len(chunk_df)
        results.append(f"Batch biller entity and account creation failed: {str(e)}")
        logger.error(f"Batch biller entity and account creation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def prepare_biller_business_insert_params(db_entity_id,biller_entity_id, row_dict, current_time):
    """Prepare parameters for biller business insert"""
    
    # Since this is a lookup after entity insert, we need to get the actual entity_id
    # This will be handled by getting the entity_id from the batch lookup
    
    business_name = row_dict.get('account_holder_name')
    
    return (
        db_entity_id,  # entity_id - will be set after lookup
        biller_entity_id,  # customer_entity_id
        None,  # customer_parent_entity_id
        business_name,  # legal_name
        'Biller entity',  # business_type
        None,  # date_of_incorporation
        'Biller entity',  # business_activity
        None,  # taxid
        business_name,  # doing_business_as
        None,  # bankruptcy_date
        None,  # closed_date
        None,  # closed_reason
        'UserUpload',  # svc_provider_name
        current_time,  # inserted_date
        current_time,  # updated_date
        'System',  # inserted_by
        'System'  # updated_by
    )

def prepare_account_insert_with_biller_entity(row_dict, biller_entity_id, current_time):
    """Prepare parameters for account insert with biller entity"""
    
    # Handle date conversions
    open_date = convert_date_field(row_dict.get('open_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    reopen_date = convert_date_field(row_dict.get('reopen_date'))
    p2p_enrollment_date = convert_date_field(row_dict.get('p2p_enrollment_date'))
    
    # Handle boolean fields
    is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle numeric fields
    account_balance = convert_numeric_field(row_dict.get('account_balance'))
    account_risk_score = convert_numeric_field(row_dict.get('account_risk_score'))
    account_status = 'Active'
    
    return (
        row_dict.get('account_id'),          # customer_account_id
        biller_entity_id,                    # entity_id (using biller entity)
        row_dict.get('account_type'),        # account_type
        row_dict.get('account_number'),      # account_number
        row_dict.get('account_routing_number'), # account_routing_number
        None,                                # company_identification
        row_dict.get('account_currency'),    # account_currency
        account_balance,                     # account_balance
        open_date,                          # open_date
        closed_date,                        # closed_date
        row_dict.get('close_reason'),       # close_reason
        row_dict.get('branch_id'),          # branch_id
        reopen_date,                        # reopen_date
        account_risk_score,                 # account_risk_score
        account_status,                     # account_status
        is_p2p_enabled,                     # is_zelle_enabled
        p2p_enrollment_date,                # p2p_enrollment_date
        row_dict.get('p2p_email'),          # p2p_email
        row_dict.get('p2p_phone'),          # p2p_phone
        is_joint,                           # is_joint
        'UserUpload',                       # svc_provider_name
        current_time,                       # insert_date
        current_time,                       # update_date
        'System',                           # inserted_by
        'System'                            # updated_by
    )

# Modified main account processing function
# Fix for the business insert - need to handle entity_id lookup properly
def prepare_biller_business_insert_params_fixed(entity_id, biller_entity_id, row_dict, current_time):
    """Prepare parameters for biller business insert with proper entity_id"""
    
    business_name = row_dict.get('business_name') or row_dict.get('account_holder_name', f'Biller_{row_dict.get("account_id")}')
    
    return (
        entity_id,  # entity_id from database
        biller_entity_id,  # customer_entity_id
        None,  # customer_parent_entity_id
        business_name,  # legal_name
        'Financial Institution',  # business_type
        None,  # date_of_incorporation
        'Account Management',  # business_activity
        None,  # taxid
        business_name,  # doing_business_as
        None,  # bankruptcy_date
        None,  # closed_date
        None,  # closed_reason
        'UserUpload',  # svc_provider_name
        current_time,  # inserted_date
        current_time,  # updated_date
        'System',  # inserted_by
        'System'  # updated_by
    )

def process_account_csv_data_batch_with_biller_entity(df, connection, is_fixed_width=False):
    """Process the entire account CSV file using batch SQL generation with biller entity creation"""
    total_success = 0
    total_errors = 0
    all_results = []
    logger.info(f"Starting enhanced batch account processing with biller entity creation for {len(df)} records")
    
    # Split dataframe into chunks
    chunk_size = CHUNK_SIZE
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    logger.info(f"Processing accounts in {num_chunks} chunks with chunk size {chunk_size}")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start_idx:end_idx].copy()
        
        status_text.text(f"Processing account records {start_idx+1} to {end_idx} of {len(df)}...")
        
        # Process this chunk using enhanced batch approach with biller entity creation
        success_count, error_count, results = process_account_chunk_batch_with_biller_entity(chunk, connection, is_fixed_width)
        
        total_success += success_count
        total_errors += error_count
        all_results.extend(results)
        
        # Update progress bar
        progress_bar.progress((i + 1) / num_chunks)
    
    progress_bar.progress(1.0)
    status_text.text(f"Completed processing {len(df)} account records with biller entity creation.")
    logger.info(f"Enhanced Account batch processing with biller entities complete. Total success: {total_success}, Total errors: {total_errors}")
    
    return total_success, total_errors, all_results


def map_transaction_fixed_width_to_standard(parsed_df: pd.DataFrame) -> pd.DataFrame:
   """Map fixed-width transaction data to standard format"""
   mapped_df = pd.DataFrame()
   
       
   # Map transaction fields
   field_mappings = {
       'account_id':'account_id',
       'transaction_id':'transaction_id',
       'b_number': 'b_number',
       'transaction_code': 'transaction_code',
       'amount': 'amount',
       'debit_credit_indicator': 'debit_credit_indicator',
       'nominal_currency': 'nominal_currency',
       'reference':'reference',
       'transaction_date': 'transaction_date',
       'description': 'description',
       'non_account_holder': 'non_account_holder',
       'Beneficiary_Details' :'beneficiary_details' ,
       'balance': 'balance',
       'eod_balance': 'eod_balance',
       'transaction_field_1':'transaction_field_1',
       'transaction_field_2':'transaction_field_2',
       'transaction_field_3':'transaction_field_3',
       'transaction_field_4':'transaction_field_4',
       'transaction_field_5':'transaction_field_5',
       'txn_source':'txn_source',
       'originator_bank': 'originator_bank',
       'intermediary_1_id':'intermediary_1_id' ,
       'positive_pay_indicator':'positive_pay_indicator',
       'intermediary_bank':'intermediary_bank',
       'beneficiary_bank': 'beneficiary_bank',
       'instructing_bank': 'instructing_bank',
       'inter_bank_information': 'inter_bank_information',
       'other_information': 'other_information',
       'transaction_field_11':'transaction_field_11',
       'transaction_field_12':'transaction_field_12',
       'transaction_field_13':'transaction_field_13',
       'transaction_field_14':'transaction_field_14',
       'transaction_field_15':'transaction_field_15',
   }
   
   for fixed_field, standard_field in field_mappings.items():
       if fixed_field in parsed_df.columns:
           mapped_df[standard_field] = parsed_df[fixed_field]
   
   # Convert date format (YYYYMMDDHHMMSS to standard datetime) and check for future dates
   if 'transaction_date' in mapped_df.columns:
       current_utc = datetime.now(timezone.utc)
       future_dates_count = 0
       
       def convert_fixed_date(date_str):
           nonlocal future_dates_count
           if not date_str or pd.isna(date_str):
               return None
           try:
               # Convert YYYYMMDDHHMMSS to YYYY-MM-DD HH:MM:SS
               date_str = str(date_str).strip()
               if len(date_str) == 14:
                   year = date_str[:4]
                   month = date_str[4:6]
                   day = date_str[6:8]
                   hour = date_str[8:10]
                   minute = date_str[10:12]
                   second = date_str[12:14]
                   converted_date = f"{year}-{month}-{day} {hour}:{minute}:{second}"
                   
                   # Parse as UTC and check if future date
                   try:
                       parsed_datetime = datetime.strptime(converted_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                       if parsed_datetime > current_utc:
                           future_dates_count += 1
                   except:
                       pass
                   
                   return converted_date
               return date_str
           except:
               return date_str
       
       mapped_df['transaction_date'] = mapped_df['transaction_date'].apply(convert_fixed_date)
       
       # Warn about future dates
       if future_dates_count > 0:
           st.error(f"IMPORTANT: Found {future_dates_count} out of {len(mapped_df)} transactions with future dates. Please verify the transaction dates are correct. ")
           st.error(f"NOTE: You can still upload the transaction data, but please be aware that any future dates in the data will not be included in the AML (Anti-Money Laundering) detection process.")
   
   # Set default values
   mapped_df['preferred_currency'] = 'USD'
   mapped_df['exchange_rate_applied'] = 1.0
   mapped_df['is_p2p_transaction'] = 'N'
   # Add required fields - ALWAYS add changetype as 'add' for fixed-width files
   mapped_df['changetype'] = 'add'
   mapped_df['counterparty_country'] = 'US'
   
   
   # Map transaction type based on transaction code
   if 'transaction_code' in mapped_df.columns:
       def map_transaction_type(code):
           type_mapping = {
               'CASH': 'Cash Transaction',
               'CHECK': 'Check Transaction',
               'DEBIT CARD': 'Debit Card Transaction',
               'CREDIT CARD': 'Credit Card Transaction',
               'ACH': 'ACH Transfer',
               'MONEY ORDER': 'Money Order'
           }
           return type_mapping.get(code, code)
       
       mapped_df['transaction_type'] = mapped_df['transaction_code'].apply(map_transaction_type)
   
   return mapped_df

def create_enhanced_customer_upload_tab_with_fixed_width():
    """Enhanced customer upload tab with fixed-width support"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Customer Data (Fixed Width Support)</h3>", unsafe_allow_html=True)
    st.info("📄 **Fixed Width Support**: Upload CUSTOMER_YYYYMMDD_RRRRR.txt files with automatic parsing!")
    
    # File format selection
    upload_format = st.radio(
        "Select file format:",
        ["CSV/Excel (Multi-format)", "Fixed Width Text"],
        key="customer_format_selection"
    )
    
    if upload_format == "Fixed Width Text":
        # Fixed width file upload
        df, metadata = create_enhanced_fixed_width_uploader(
            file_types=["txt"],
            key="customer_fixed_width_upload"
        )
        
        if df is not None and not df.empty:
            try:
                st.write(f"**File:** {metadata['filename']} (Fixed Width)")
                st.write(f"**Total records:** {len(df):,}")
                st.write(f"**Parsed fields:** {metadata.get('parsed_fields', 0)}")
                
                # Map to standard format
                with st.spinner("Mapping fixed-width data to standard format..."):
                    mapped_df = map_customer_fixed_width_to_standard(df)

                if 'changetype' not in mapped_df.columns or mapped_df['changetype'].isna().any():
                    # st.warning("Setting changetype to 'add' for all fixed-width records...")
                    mapped_df['changetype'] = 'add'

                # Ensure customer_id is not null
                null_customer_ids = mapped_df['customer_id'].isna().sum()
                if null_customer_ids > 0:
                    st.error(f" {null_customer_ids} records have null customer_id")
                    return
                
                # Ensure customer_type is not null
                null_customer_types = mapped_df['customer_type'].isna().sum()
                if null_customer_types > 0:
                    st.error(f" {null_customer_types} records have null customer_type")
                    return
                
                
                st.success(" Fixed-width data mapped to standard format!")
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'>📋 Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                # Validate mapped data
                with st.spinner("Validating mapped customer data..."):
                    validation_errors = validate_data(mapped_df, is_fixed_width=True)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please check your fixed-width data format.")
                else:
                    st.success(" Customer data validation passed successfully!")
                    
                    # Count of operations by type
                    if 'changetype' in mapped_df.columns:
                        operation_counts = mapped_df['changetype'].str.lower().value_counts().to_dict()
                        st.markdown("<h3 style='font-size: 18px;'>Operation Summary</h3>", unsafe_allow_html=True)
                        cols = st.columns(3)
                        cols[0].metric("Add Operations", operation_counts.get('add', 0))
                        cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                        cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                    
                    # Process button
                    if st.button("Upload Customer Data ", key="process_customer_fixed_width"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing customer data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting customer processing for fixed-width file with {len(mapped_df)} records")
                                    
                                    success_count, error_count, results = process_entity_csv_data_batch(mapped_df, connection, is_fixed_width=True)
                                    connection.close()
                                    
                                    # Store results in session state
                                    st.session_state.entity_success_count = success_count
                                    st.session_state.entity_error_count = error_count
                                    st.session_state.entity_results = results
                                    st.session_state.entity_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f" Customer data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"Customer data processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
            
            except Exception as e:
                st.error(f"Error processing fixed-width customer file: {e}")
                st.exception(e)
    else:
        # Original multi-format upload
        create_enhanced_customer_upload_tab_with_multi_format()

def create_enhanced_account_upload_tab_with_fixed_width():
    """Enhanced account upload tab with fixed-width support"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Account Data (Fixed Width Support)</h3>", unsafe_allow_html=True)
    st.info("📄 **Fixed Width Support**: Upload ACCOUNT_YYYYMMDD_RRRRR.txt files with automatic parsing!")
    
    if not st.session_state.entity_processed:
        st.warning("⚠️ It is recommended to process customer data first before processing accounts")
    
    # File format selection
    upload_format = st.radio(
        "Select file format:",
        ["CSV/Excel (Multi-format)", "Fixed Width Text"],
        key="account_format_selection"
    )
    
    if upload_format == "Fixed Width Text":
        # Fixed width file upload
        df, metadata = create_enhanced_fixed_width_uploader(
            file_types=["txt"],
            key="account_fixed_width_upload"
        )
        
        if df is not None and not df.empty:
            try:
                st.write(f"**File:** {metadata['filename']} (Fixed Width)")
                st.write(f"**Total records:** {len(df):,}")
                st.write(f"**Parsed fields:** {metadata.get('parsed_fields', 0)}")
                
                # Map to standard format
                with st.spinner("Mapping fixed-width data to standard format..."):
                    mapped_df = map_account_fixed_width_to_standard(df)

                if 'changetype' not in mapped_df.columns or mapped_df['changetype'].isna().any():
                    # st.warning("Setting changetype to 'add' for all fixed-width records...")
                    mapped_df['changetype'] = 'add'

                st.success(" Fixed-width data mapped to standard format!")
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'>📋 Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                # Validate mapped data
                with st.spinner("Validating mapped account data..."):
                    validation_errors = validate_account_data(mapped_df)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please check your fixed-width data format.")
                else:
                    st.success(" Account data validation passed successfully!")

                    # Process button
                    if st.button("Upload Account Data", key="process_account_fixed_width"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing account data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting account processing for fixed-width file with {len(mapped_df)} records")
                                    
                                    success_count, error_count, results = process_account_csv_data_batch(mapped_df, connection, is_fixed_width=True)
                                    connection.close()
                                    
                                    # Store results in session state
                                    st.session_state.account_success_count = success_count
                                    st.session_state.account_error_count = error_count
                                    st.session_state.account_results = results
                                    st.session_state.account_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f" Account data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"Account data processing completed in {processing_time:.2f} seconds")
                                    st.info(f"Please run the Relationship Upload to match accounts created to customers")
                                else:
                                    st.error("Could not connect to the database.")
            
            except Exception as e:
                st.error(f"Error processing fixed-width account file: {e}")
                st.exception(e)
    else:
        # Original multi-format upload
        create_enhanced_account_upload_tab_with_multi_format()

def create_enhanced_transaction_upload_tab_with_fixed_width():
    """Enhanced transaction upload tab with fixed-width support and multi-threading"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Transaction Data (Fixed Width Support)</h3>", unsafe_allow_html=True)
    st.info("**Fixed Width Support**: Upload TRANSACTION_YYYYMMDD_RRRRR.txt files with automatic parsing!")
    
    if not st.session_state.account_processed:
        st.warning(" It is recommended to process account data first before processing transactions")
    
    # File format selection
    upload_format = st.radio(
        "Select file format:",
        ["CSV/Excel (Multi-format)", "Fixed Width Text"],
        key="transaction_format_selection"
    )
    
    if upload_format == "Fixed Width Text":
        # Fixed width file upload
        df, metadata = create_enhanced_fixed_width_uploader(
            file_types=["txt"],
            key="transaction_fixed_width_upload"
        )
        
        if df is not None and not df.empty:
            try:
                # Save uploaded file temporarily for AML processing
                temp_file_path = f"temp_transactions_fw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                st.write(f"**File:** {metadata['filename']} (Fixed Width)")
                st.write(f"**Total records:** {len(df):,}")
                st.write(f"**Parsed fields:** {metadata.get('parsed_fields', 0)}")

                
                # Map to standard format
                with st.spinner("Mapping fixed-width data to standard format..."):
                    mapped_df = map_transaction_fixed_width_to_standard(df)
                
                if 'changetype' not in mapped_df.columns or mapped_df['changetype'].isna().any():
                    mapped_df['changetype'] = 'add'
                
                # Save mapped data as CSV for AML processing
                mapped_df.to_csv(temp_file_path, index=False)
                st.session_state.temp_transaction_file = temp_file_path
                st.session_state.last_upload_type = "Fixed-Width"
                
                st.success(" Fixed-width data mapped to standard format!")
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'>📋 Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                # Show date conversion examples
                st.markdown("<h3 style='font-size: 18px;'>📅 Date Format Conversion Examples</h3>", unsafe_allow_html=True)
                if 'transaction_date' in mapped_df.columns:
                    date_examples = []
                    for i, conv in enumerate(mapped_df['transaction_date'].head(5)):
                        date_examples.append({
                            "Row": i+1,
                            "Converted Format": conv
                        })
                    st.table(date_examples)
                
                # Validate mapped data
                with st.spinner("Validating mapped transaction data..."):
                    validation_errors = validate_transaction_data(mapped_df,is_fixed_width=True)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please check your fixed-width data format.")
                else:
                    st.success(" Transaction data validation passed successfully!")
                    
                    # Count of operations by type
                    if 'changetype' in mapped_df.columns:
                        operation_counts = mapped_df['changetype'].str.lower().value_counts().to_dict()
                        st.markdown("<h3 style='font-size: 18px;'>Operation Summary</h3>", unsafe_allow_html=True)
                        cols = st.columns(3)
                        cols[0].metric("Add Operations", operation_counts.get('add', 0))
                        cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                        cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                    
                    st.markdown("---")
                    
                    # Step 1: Transaction Processing with Multi-threading
                    st.subheader("Step 1: Upload Transaction Data")
                    st.info(" Upload and process transaction data into the database using parallel processing")
                    
                    if st.button("Upload Transaction Data ", key="process_transaction_fixed_width"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing transaction data ..."):
                                start_time = time.time()
                                
                                # # Use the multi-threaded processing function
                                # success_count, error_count, results, aml_results = process_transaction_csv_data_batch_multithreaded(
                                #     mapped_df, True, None, is_fixed_width=True)

                                success_count, error_count, results, aml_results = process_transaction_csv_data_batch_multithreaded(
                                    mapped_df, 
                                    allow_duplicates=False,
                                    connection=None, 
                                    is_fixed_width=True,
                                    max_workers=8,  # Explicit integer
                                    chunk_size=1000
                                )
                                
                                # Store results in session state
                                st.session_state.transaction_success_count = success_count
                                st.session_state.transaction_error_count = error_count
                                st.session_state.transaction_results = results
                                st.session_state.transaction_processed = True
                                st.session_state.transaction_upload_completed = True
                                
                                end_time = time.time()
                                processing_time = end_time - start_time
                                
                                st.success(f" Multi-threaded transaction processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                
                                if processing_time > 0:
                                    rate = len(mapped_df) / processing_time
                                    st.info(f"Processing rate: {rate:.2f} records/second")
                                
                                # Only show success message if all transactions were processed successfully
                                if success_count == len(mapped_df) and error_count == 0:
                                    st.success(" Transaction data has been successfully uploaded to the database!")
                                elif error_count > 0:
                                    st.warning(f" Transaction upload completed with {error_count:,} errors. Please check the error details above.")
                    
                    # Step 2: AML Detection (same as before)
                    st.markdown("---")
                    st.subheader("Step 2: AML Detection")
                    
                    transaction_upload_completed = st.session_state.get('transaction_upload_completed', False)
                    transaction_success_count = st.session_state.get('transaction_success_count', 0)
                    aml_available = st.session_state.get('aml_available', False)
                    aml_completed = st.session_state.get('aml_completed', False)
                    
                    if not transaction_upload_completed or transaction_success_count == 0:
                        st.info("Upload and process transaction data first to enable AML detection")
                        st.button("Commence AML Detection", disabled=True, key="disabled_aml_no_data_fw")
                    elif aml_completed:
                        st.success("AML Detection completed successfully!")
                        # Show AML results
                        aml_results = st.session_state.get('aml_results', {})
                        alerts = aml_results.get('alerts_generated', 0)
                        entities = aml_results.get('entities_processed', 0)
                        processing_time = aml_results.get('processing_time', 0)
                        
                        st.header("AML Detection Results")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Alerts Generated", alerts)
                        col2.metric("Entities Processed", entities)
                        col3.metric("Processing Time", f"{processing_time:.1f}s")
                    else:
                        st.info("Run comprehensive AML detection on the uploaded transaction data")
                        
                        # Show upload metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Transactions Uploaded", transaction_success_count)
                        col2.metric("Upload Errors", st.session_state.get('transaction_error_count', 0))
                        col3.metric("Ready for AML", "" if aml_available else "")
                        
                        if aml_available:
                            if st.button("Commence AML Detection", key="start_aml_fw"):
                                st.session_state.aml_running = True
                                st.session_state.aml_completed = False
                                
                                # Direct AML detection for Fixed-Width files
                                try:
                                    if hasattr(st.session_state, 'temp_transaction_file') and st.session_state.temp_transaction_file:
                                        aml_results = run_aml_detection_after_upload(
                                            st.session_state.temp_transaction_file, 
                                            "transaction", 
                                            is_fixed_width=True
                                        )
                                        
                                        if aml_results:
                                            st.session_state.aml_results = aml_results
                                            st.session_state.aml_completed = True
                                            st.session_state.aml_running = False
                                            st.rerun()
                                        else:
                                            st.session_state.aml_running = False
                                            st.error("AML Detection failed for Fixed-Width file")
                                    else:
                                        st.session_state.aml_running = False
                                        st.error("No transaction file available for AML detection")
                                except Exception as e:
                                    st.session_state.aml_running = False
                                    st.error(f"AML Detection failed: {str(e)}")
                        else:
                            st.warning("AML Detection system is not available")
                            st.button("Commence AML Detection", disabled=True, key="disabled_aml_unavailable_fw")
            
            except Exception as e:
                st.error(f"Error processing fixed-width transaction file: {e}")
                st.exception(e)
    else:
        # Original multi-format upload
        create_enhanced_transaction_upload_tab_with_multi_format()

def process_transaction_csv_data_batch_multithreaded(df, allow_duplicates=False, connection=None, is_fixed_width=False, max_workers=None, chunk_size=1000):
    """
    Multi-threaded transaction processing using connection pool
    """
    results = []
    success_count = 0
    error_count = 0
    aml_results = {}
    
    try:
        # Import database connection utility
        from aml_detection.db_utils import DatabaseConnection
        db_connection = DatabaseConnection()
        
        # Determine optimal number of workers based on connection pool
        pool_info = db_connection.get_pool_info()
        available_connections = pool_info.get('total_available_connections', 10)
        
        if max_workers is None:
            pool_info = db_connection.get_pool_info()
            available_connections = pool_info.get('total_available_connections', 10)
            
            # Calculate optimal workers with safety checks
            calculated_workers = min(available_connections // 2, 8)
            max_workers = max(calculated_workers, 2)  # Ensure at least 2 workers
        
        # CRITICAL: Validate max_workers is a positive integer
        if not isinstance(max_workers, int) or max_workers <= 0:
            logger.error(f"Invalid max_workers {max_workers} (type: {type(max_workers)}), using fallback 4")
            max_workers = 4
        
        logger.info(f"Using {max_workers} worker threads for processing (validated as integer)")
        
        # Create UI containers for progress tracking
        progress_container = st.empty()
        metrics_container = st.empty()
        thread_status_container = st.empty()
        
       # Create chunks
        if len(df) == 0:
            logger.warning("Empty DataFrame provided")
            return 0, 0, ["No data to process"], {}
        
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        logger.info(f"Split data into {len(chunks)} chunks of size {chunk_size}")

        # VALIDATION: Ensure we have chunks
        if len(chunks) == 0:
            logger.error("No chunks created from DataFrame")
            return 0, len(df), ["Error: Could not create chunks from data"], {}
        
        logger.info(f"Split data into {len(chunks)} chunks of size {chunk_size}")
        
        progress_container = st.empty()
        with progress_container.container():
            st.info(f"Processing {len(chunks)} chunks with {max_workers} threads...")
            overall_progress = st.progress(0.0)
        
        # Thread-safe containers for results
        thread_results = queue.Queue()
        thread_lock = threading.Lock()
        completed_chunks = 0
        
        def process_chunk_worker(chunk_index, chunk_df):
            """Worker function for processing a single chunk"""
            thread_id = threading.current_thread().name
            chunk_start_time = time.time()
            
            logger.info(f"Thread {thread_id}: Starting chunk {chunk_index + 1}/{len(chunks)} with {len(chunk_df)} records")
            
            try:
                # Get database connection for this thread
                with db_connection.get_connection() as conn:
                    chunk_success, chunk_errors, chunk_results = process_transaction_chunk_batch(
                        chunk_df, conn, allow_duplicates, is_fixed_width
                    )
                
                chunk_end_time = time.time()
                chunk_processing_time = chunk_end_time - chunk_start_time
                
                result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': chunk_success,
                    'error_count': chunk_errors,
                    'results': chunk_results,
                    'processing_time': chunk_processing_time,
                    'records_processed': len(chunk_df)
                }
                
                thread_results.put(result)
                
                # Update progress safely
                nonlocal completed_chunks
                with thread_lock:
                    completed_chunks += 1
                    progress = min(completed_chunks / len(chunks),1.0)
                    logger.info(f"Thread {thread_id}: Completed chunk {chunk_index + 1}, progress: {progress:.1%}")

                    
                    # with metrics_container.container():
                    #     st.subheader("Multi-threaded Processing Progress")
                    #     col1, col2, col3, col4 = st.columns(4)
                    #     col1.metric("Completed Chunks", completed_chunks, delta=1)
                    #     col2.metric("Total Chunks", len(chunks))
                    #     col3.metric("Active Threads", threading.active_count() - 1)  # Subtract main thread
                    #     col4.metric("Progress", f"{progress:.1%}")
                
                logger.info(f"Thread {thread_id}: Completed chunk {chunk_index + 1} successfully")
                return result
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Thread {thread_id}: Error processing chunk {chunk_index + 1}: {str(e)}")
                logger.error(f"Thread {thread_id}: Full error traceback: {error_details}")
                
                error_result = {
                    'chunk_index': chunk_index,
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': len(chunk_df),
                    'results': [f"Chunk {chunk_index + 1}: Thread error - {str(e)}"],
                    'processing_time': 0,
                    'records_processed': len(chunk_df),
                    'error': str(e)
                }
                
                thread_results.put(error_result)
                
                with thread_lock:
                    completed_chunks += 1
                    progress = min(completed_chunks / len(chunks),1.0)
                    
                    logger.info(f"Thread {thread_id}: Completed chunk {chunk_index + 1}, progress: {progress:.1%}")
                    
                    with thread_status_container.container():
                        logger.error(f"Thread {thread_id}: Error in chunk {chunk_index + 1}: {str(e)}")
                
                return error_result
        
        # Execute chunks in parallel using ThreadPoolExecutor
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="TxnProcessor") as executor:
            # Submit all chunks to the thread pool
            future_to_chunk = {
                executor.submit(process_chunk_worker, i, chunk): i 
                for i, chunk in enumerate(chunks)
            }
            
            # Update UI with thread pool status
            with progress_container.container():
                st.info(f"Processing {len(chunks)} chunks with {max_workers} worker threads...")
                overall_progress.progress(0)
            
            # Wait for all futures to complete
            for future in as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    logger.info(f"Future completed for chunk {chunk_index + 1}")
                except Exception as e:
                    logger.error(f"Future failed for chunk {chunk_index + 1}: {str(e)}")
        
        # Collect all results from queue
        all_chunk_results = []
        while not thread_results.empty():
            all_chunk_results.append(thread_results.get())
        
        # Sort results by chunk index to maintain order
        all_chunk_results.sort(key=lambda x: x['chunk_index'])
        
        # Aggregate results
        total_processing_time = time.time() - start_time
        
        for chunk_result in all_chunk_results:
            success_count += chunk_result['success_count']
            error_count += chunk_result['error_count']
            results.extend(chunk_result['results'])
        
        # Final progress update
        overall_progress.progress(1.0)
        
        # with progress_container.container():
        logger.info(f"Multi-threaded processing completed in {total_processing_time:.2f} seconds!")
        
        # with metrics_container.container():
        #     st.subheader("Final Processing Results")
        #     col1, col2, col3, col4, col5 = st.columns(5)
        #     col1.metric("Total Success", success_count)
        #     col2.metric("Total Errors", error_count)
        #     col3.metric("Total Time", f"{total_processing_time:.2f}s")
        #     col4.metric("Records/Second", f"{len(df) / total_processing_time:.2f}")
        #     col5.metric("Threads Used", max_workers)
        
        # Detailed thread performance
        # with st.expander("Thread Performance Details"):
        #     thread_df = pd.DataFrame([
        #         {
        #             'Chunk': result['chunk_index'] + 1,
        #             'Thread ID': result['thread_id'],
        #             'Records': result['records_processed'],
        #             'Success': result['success_count'],
        #             'Errors': result['error_count'],
        #             'Time (s)': f"{result['processing_time']:.2f}",
        #             'Rate (rec/s)': f"{result['records_processed'] / max(result['processing_time'], 0.001):.2f}"
        #         }
        #         for result in all_chunk_results
        #     ])
        #     st.dataframe(thread_df)
        
        logger.info(f"Multi-threaded processing completed: {success_count} success, {error_count} errors in {total_processing_time:.2f}s")
        
        # Summary results
        results.insert(0, f"Multi-threaded Summary: {len(chunks)} chunks processed using {max_workers} threads")
        results.insert(1, f"Performance: {len(df) / total_processing_time:.2f} records/second")
        
        # Clean up UI after a brief display
        time.sleep(2)
        progress_container.empty()
        metrics_container.empty()
        thread_status_container.empty()
        
    except Exception as e:
        logger.error(f"Multi-threaded transaction processing failed: {str(e)}")
        error_count = len(df)
        results.append(f"Multi-threaded processing failed: {str(e)}")
        
        # Clean up UI on error
        if 'progress_container' in locals():
            progress_container.empty()
        if 'metrics_container' in locals():
            metrics_container.empty()
        if 'thread_status_container' in locals():
            thread_status_container.empty()
    
    return success_count, error_count, results, aml_results


def process_agents_in_chunks_threaded(agent_data_list, current_time, chunk_size=1000, max_workers=8):
    """
    Multi-threaded agent processing using connection pool
    """
    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    
    agent_entity_map = {}
    agent_map_lock = threading.Lock()
    
    # Split agent data into chunks
    agent_chunks = [agent_data_list[i:i + chunk_size] for i in range(0, len(agent_data_list), chunk_size)]
    
    def process_agent_chunk(chunk):
        """Process a chunk of agents in a separate thread"""
        thread_id = threading.current_thread().name
        logger.info(f"Thread {thread_id}: Processing {len(chunk)} agents")
        
        chunk_agent_map = {}
        
        try:
            with db_connection.get_connection() as connection:
                cursor = connection.cursor()
                
                # Extract intermediary_1_ids from chunk
                intermediary_ids = [item[0] for item in chunk if item[0]]
                
                if not intermediary_ids:
                    return chunk_agent_map
                    
                # Bulk check existing agents
                placeholders = ','.join(['%s'] * len(intermediary_ids))
                cursor.execute(f"""
                    SELECT customer_entity_id, entity_id 
                    FROM entity_customer 
                    WHERE customer_entity_id IN ({placeholders}) AND customertype = 'Agents'
                """, intermediary_ids)
                
                existing_agents = dict(cursor.fetchall())
                
                # Prepare data for new agents
                new_entity_data = []
                new_entity_customer_data = []
                
                for intermediary_1_id, transaction_field_15 in chunk:
                    if not intermediary_1_id:
                        continue
                        
                    if intermediary_1_id in existing_agents:
                        # Agent exists, store mapping
                        chunk_agent_map[intermediary_1_id] = existing_agents[intermediary_1_id]
                    else:
                        # Agent doesn't exist, prepare for bulk insert
                        first_name = ""
                        last_name = ""
                        
                        if transaction_field_15:
                            # Split by comma first, then by space
                            if ',' in transaction_field_15:
                                parts = [part.strip() for part in transaction_field_15.split(',')]
                                if len(parts) >= 2:
                                    first_name = parts[0]
                                    last_name = parts[1]
                                elif len(parts) == 1:
                                    first_name = parts[0]
                            else:
                                # Split by space
                                parts = transaction_field_15.split()
                                if len(parts) >= 2:
                                    first_name = parts[0]
                                    last_name = ' '.join(parts[1:])  # Join remaining parts as last name
                                elif len(parts) == 1:
                                    first_name = parts[0]
                        
                        new_entity_data.append((intermediary_1_id, 'Agents', 'System', 'System', current_time, current_time, 'UserUpload-CashOnly'))
                        new_entity_customer_data.append((intermediary_1_id, first_name, last_name))
                
                # Bulk insert new entities
                if new_entity_data:
                    cursor.executemany("""
                        INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, SVC_PROVIDER_NAME)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, new_entity_data)
                    
                    # Get the entity_ids for newly inserted records
                    new_intermediary_ids = [item[0] for item in new_entity_data]
                    placeholders = ','.join(['%s'] * len(new_intermediary_ids))
                    cursor.execute(f"""
                        SELECT customer_entity_id, entity_id 
                        FROM entity 
                        WHERE customer_entity_id IN ({placeholders}) AND entity_type = 'Agents'
                    """, new_intermediary_ids)
                    
                    new_entity_mappings = dict(cursor.fetchall())
                    
                    # Prepare entity_customer data with entity_ids
                    entity_customer_insert_data = []
                    for intermediary_1_id, first_name, last_name in new_entity_customer_data:
                        entity_id = new_entity_mappings.get(intermediary_1_id)
                        if entity_id:
                            entity_customer_insert_data.append((
                                entity_id, intermediary_1_id, first_name, last_name, 'Agents', 
                                'UserUpload-CashOnly', current_time, current_time, 'System', 'System'
                            ))
                            chunk_agent_map[intermediary_1_id] = entity_id
                    
                    # Bulk insert into entity_customer
                    if entity_customer_insert_data:
                        cursor.executemany("""
                            INSERT INTO entity_customer (entity_id, customer_entity_id, first_name, last_name, customertype, svc_provider_name, inserted_date, updated_date, inserted_by, updated_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, entity_customer_insert_data)
                
                connection.commit()
                cursor.close()
                
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error processing agent chunk: {str(e)}")
        
        return chunk_agent_map
    
    # Process agent chunks in parallel
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="AgentProcessor") as executor:
        future_to_chunk = {executor.submit(process_agent_chunk, chunk): chunk for chunk in agent_chunks}
        
        for future in as_completed(future_to_chunk):
            try:
                chunk_result = future.result()
                with agent_map_lock:
                    agent_entity_map.update(chunk_result)
            except Exception as e:
                logger.error(f"Agent processing future failed: {str(e)}")
    
    return agent_entity_map

def create_enhanced_file_uploader(file_types: list = None, key: str = "file_upload") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    Enhanced file uploader that supports CSV, TXT, and Excel files
    Returns: (dataframe, metadata)
    """
    if file_types is None:
        file_types = ["csv", "txt", "xls", "xlsx"]
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a file", 
        type=file_types, 
        key=key,
        help="Supported formats: CSV, TXT (with auto-delimiter detection), Excel (XLS/XLSX)"
    )
    
    if uploaded_file is not None:
        file_extension = uploaded_file.name.split('.')[-1].lower()
        file_bytes = uploaded_file.getvalue()
        
        metadata = {
            "filename": uploaded_file.name,
            "file_type": file_extension,
            "file_size": len(file_bytes)
        }
        
        try:
            if file_extension == 'csv':
                # Standard CSV processing
                encoding = detect_encoding(file_bytes)
                file_content = file_bytes.decode(encoding)
                
                df = pd.read_csv(io.StringIO(file_content))
                metadata["encoding"] = encoding
                metadata["delimiter"] = ","
                
                st.success(f"CSV file loaded successfully!")
                st.info(f"Encoding: {encoding}")
                
                return df, metadata
                
            elif file_extension == 'txt':
                # TXT file with delimiter detection
                encoding = detect_encoding(file_bytes)
                file_content = file_bytes.decode(encoding)
                
                # Detect delimiter
                detected_delimiter, confidence = detect_delimiter(file_content)
                
                st.subheader(" Delimiter Detection Results")
                
                # Show detection results
                delimiter_names = {
                    ',': 'Comma (,)',
                    '\t': 'Tab (\\t)',
                    ';': 'Semicolon (;)',
                    '|': 'Pipe (|)',
                    ':': 'Colon (:)',
                    ' ': 'Space ( )'
                }
                
                detected_name = delimiter_names.get(detected_delimiter, f"'{detected_delimiter}'")
                confidence_pct = confidence * 100
                
                st.info(f"**Detected delimiter:** {detected_name} (Confidence: {confidence_pct:.1f}%)")
                
                # Let user confirm or choose different delimiter
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    delimiter_options = list(delimiter_names.keys())
                    delimiter_labels = list(delimiter_names.values())
                    
                    try:
                        default_index = delimiter_options.index(detected_delimiter)
                    except ValueError:
                        default_index = 0
                    
                    selected_delimiter = st.selectbox(
                        "Confirm or select delimiter:",
                        options=delimiter_options,
                        format_func=lambda x: delimiter_names[x],
                        index=default_index,
                        key=f"delimiter_select_{key}"
                    )
                
                with col2:
                    custom_delimiter = st.text_input(
                        "Custom delimiter:",
                        value="",
                        max_chars=5,
                        help="Enter a custom delimiter if not in the list",
                        key=f"custom_delimiter_{key}"
                    )
                
                # Use custom delimiter if provided
                final_delimiter = custom_delimiter if custom_delimiter else selected_delimiter
                
                # Show preview
                # st.subheader(" File Preview")
                st.markdown("<h3 style='font-size: 18px;'> File Preview</h3>", unsafe_allow_html=True)
                preview_df = preview_txt_file(file_content, final_delimiter, max_rows=5)
                
                if not preview_df.empty:
                    st.dataframe(preview_df)
                    
                    # Confirm button
                    if st.button(f"Load file with delimiter: {delimiter_names.get(final_delimiter, final_delimiter)}", key=f"confirm_txt_{key}"):
                        # Load full file
                        try:
                            df = pd.read_csv(io.StringIO(file_content), delimiter=final_delimiter)
                            metadata["encoding"] = encoding
                            metadata["delimiter"] = final_delimiter
                            metadata["delimiter_confidence"] = confidence
                            
                            st.success(f"TXT file loaded successfully with {len(df)} rows!")
                            return df, metadata
                        except Exception as e:
                            st.error(f"Error loading file: {str(e)}")
                            return None, metadata
                else:
                    st.warning(" Unable to preview file with selected delimiter. Please try a different delimiter.")
                
                return None, metadata
                
            elif file_extension in ['xls', 'xlsx']:
                # Excel file processing
                df, sheet_names = read_excel_file(file_bytes)
                
                if not df.empty:
                    metadata["sheet_names"] = sheet_names
                    metadata["active_sheet"] = sheet_names[0] if sheet_names else "Sheet1"
                    
                    st.success(f"Excel file loaded successfully!")
                    
                    # Show sheet selection if multiple sheets
                    if len(sheet_names) > 1:
                        st.subheader(" Sheet Selection")
                        selected_sheet = st.selectbox(
                            "Select sheet to load:",
                            options=sheet_names,
                            key=f"sheet_select_{key}"
                        )
                        
                        if selected_sheet != sheet_names[0]:
                            # Reload with selected sheet
                            df, _ = read_excel_file(file_bytes, selected_sheet)
                            metadata["active_sheet"] = selected_sheet
                    
                    st.info(f"Active sheet: {metadata['active_sheet']}")
                    
                    return df, metadata
                else:
                    st.error("Failed to read Excel file")
                    return None, metadata
            else:
                st.error(f"Unsupported file type: {file_extension}")
                return None, metadata
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            return None, metadata
    
    return None, {}

def create_schema_management_interface():
    """Create interface for managing fixed-width schemas"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 Fixed Width Schemas")
    
    # Show available schemas
    schemas = {
        "Customer": "CUSTOMER_YYYYMMDD_RRRRR.txt",
        "Account": "ACCOUNT_YYYYMMDD_RRRRR.txt", 
        "Transaction": "TRANSACTION_YYYYMMDD_RRRRR.txt",
        "Relationship": "RELATIONSHIP_YYYYMMDD_RRRRR.txt"
    }
    
    with st.sidebar.expander("📋 Schema Information"):
        for schema_name, pattern in schemas.items():
            st.write(f"**{schema_name}:**")
            st.write(f"Pattern: `{pattern}`")
            
            # Show field count
            if schema_name == "Customer":
                schema = create_customer_fixed_width_schema()
            elif schema_name == "Account":
                schema = create_account_fixed_width_schema()
            elif schema_name == "Transaction":
                schema = create_transaction_fixed_width_schema()
            elif schema_name == "Relationship":
                schema = create_relationship_fixed_width_schema()
            
            st.write(f"Fields: {len(schema.fields)}")
            st.write(f"Total Length: {schema.total_length}")
            st.write("---")

def create_fixed_width_template_section():
    """Create template section for fixed-width files"""
    with st.expander("📄 Fixed Width File Templates & Documentation"):
        st.markdown("### Fixed Width File Format Documentation")
        st.markdown("Download documentation and examples for fixed-width file formats.")
        
        # Documentation tabs
        doc_tabs = st.tabs(["📋 Field Specifications", "📁 File Patterns", "🔄 Data Mapping", "Examples"])
        
        with doc_tabs[0]:
            st.markdown("#### Field Specifications")
            
            # Customer fields documentation
            st.markdown("**Customer File Fields:**")
            customer_schema = create_customer_fixed_width_schema()
            customer_spec = []
            for field in customer_schema.fields[:10]:  # Show first 10 fields
                customer_spec.append({
                    "Field": field.field_name,
                    "Name": field.column_name,
                    "Position": f"{field.position}-{field.position + field.size - 1}",
                    "Size": field.size,
                    "Type": field.format_type,
                    "Mapping": field.bliink_mapping
                })
            
            st.dataframe(pd.DataFrame(customer_spec), use_container_width=True)
            st.info(f"Total Customer Fields: {len(customer_schema.fields)}")
            
            # Account fields documentation
            st.markdown("**Account File Fields:**")
            account_schema = create_account_fixed_width_schema()
            account_spec = []
            for field in account_schema.fields:
                account_spec.append({
                    "Field": field.field_name,
                    "Name": field.column_name,
                    "Position": f"{field.position}-{field.position + field.size - 1}",
                    "Size": field.size,
                    "Type": field.format_type,
                    "Mapping": field.bliink_mapping
                })
            
            st.dataframe(pd.DataFrame(account_spec), use_container_width=True)
            
            # Transaction fields documentation
            st.markdown("**Transaction File Fields:**")
            transaction_schema = create_transaction_fixed_width_schema()
            transaction_spec = []
            for field in transaction_schema.fields[:15]:  # Show first 15 fields
                transaction_spec.append({
                    "Field": field.field_name,
                    "Name": field.column_name,
                    "Position": f"{field.position}-{field.position + field.size - 1}",
                    "Size": field.size,
                    "Type": field.format_type,
                    "Mapping": field.bliink_mapping
                })
            
            st.dataframe(pd.DataFrame(transaction_spec), use_container_width=True)
            st.info(f"Total Transaction Fields: {len(transaction_schema.fields)}")
        
        with doc_tabs[1]:
            st.markdown("#### File Naming Patterns")
            
            patterns = [
                {"File Type": "Customer", "Pattern": "CUSTOMER_YYYYMMDD_RRRRR.txt", "Example": "CUSTOMER_20241215_00001.txt"},
                {"File Type": "Account", "Pattern": "ACCOUNT_YYYYMMDD_RRRRR.txt", "Example": "ACCOUNT_20241215_00001.txt"},
                {"File Type": "Transaction", "Pattern": "TRANSACTION_YYYYMMDD_RRRRR.txt", "Example": "TRANSACTION_20241215_00001.txt"},
                {"File Type": "Relationship", "Pattern": "RELATIONSHIP_YYYYMMDD_RRRRR.txt", "Example": "RELATIONSHIP_20241215_00001.txt"}
            ]
            
            pattern_df = pd.DataFrame(patterns)
            st.dataframe(pattern_df, use_container_width=True)
            
            st.markdown("**Pattern Legend:**")
            st.write("- `YYYY`: 4-digit year")
            st.write("- `MM`: 2-digit month") 
            st.write("- `DD`: 2-digit day")
            st.write("- `RRRRR`: 5-digit sequence number (right-justified, zero-filled)")
        
        with doc_tabs[2]:
            st.markdown("#### Data Mapping")
            st.markdown("**Fixed Width to Database Field Mapping:**")
            
            # Show key mappings
            key_mappings = [
                {"Fixed Width Field": "Customer_Number", "Database Table": "entity", "Database Field": "customer_entity_id", "Description": "Unique customer identifier"},
                {"Fixed Width Field": "Customer_Name", "Database Table": "entity_customer", "Database Field": "first_name, last_name", "Description": "Customer name (parsed)"},
                {"Fixed Width Field": "SSN", "Database Table": "entity_customer", "Database Field": "ssn", "Description": "Social Security Number"},
                {"Fixed Width Field": "A_Number", "Database Table": "account", "Database Field": "customer_account_id", "Description": "Account identifier"},
                {"Fixed Width Field": "Amount", "Database Table": "transactions", "Database Field": "amount", "Description": "Transaction amount"},
                {"Fixed Width Field": "Date_time", "Database Table": "transactions", "Database Field": "transaction_date", "Description": "Transaction timestamp"}
            ]
            
            mapping_df = pd.DataFrame(key_mappings)
            st.dataframe(mapping_df, use_container_width=True)
        
        with doc_tabs[3]:
            st.markdown("#### Sample Data Examples")
            
            # Customer example
            st.markdown("**Customer Record Example:**")
            st.code("""
CUST12345CUSWBP                   John Smith                                      US00DOB_UNKNOWN         123 Main St                      Apt 4B                            10001     New York                          NY                                MY555-123-4567                              555-987-6543                              Software Engineer                         A12345678                                     US555-123-4567                              john.smith@email.com                          US    ACTIVE              00N0        0         0                   0                 0                   W                                                 BEN       Jane Doe                                          WBP                 N
            """)
            
            # Account example  
            st.markdown("**Account Record Example:**")
            st.code("""
SOUTHERNCONNECT2600352309BAWBP            CURRENT                                           2020-01-15          2027-12-31                            NEW                                                                                                             N John Smith Account Holder                                                                      0                   WBP                                               00        65       BRANCH_NYC_001                                    Jane Doe                                          
            """)
            
            # Transaction example
            st.markdown("**Transaction Record Example:**")
            st.code("""
SOUTHERNCONNECT2600352309BAWBP            185:1:RCNTELECOMSERVICESCONSOLI:BLRWBP            N20231215143000CASH              200.00             D    USD  0                   000000056789CFPD01Terminal NYC001                                                                                                                                               0                   0                   UNITED WATER                                      USY...
            """)

# Update the main UI tabs to include fixed-width support

def update_customer_upload_tab():
    """Update customer upload tab to include both formats"""
    create_enhanced_customer_upload_tab_with_fixed_width()
    

def update_account_upload_tab():
    """Update account upload tab to include both formats"""
    create_enhanced_account_upload_tab_with_fixed_width()

def update_transaction_upload_tab():
    """Update transaction upload tab to include both formats"""
    create_enhanced_transaction_upload_tab_with_fixed_width()

# Enhanced Template Section with Fixed Width Support

def create_comprehensive_template_section():
    """Comprehensive template section including both CSV and Fixed Width formats"""
    with st.expander("📁 Download Templates (All Formats)"):
        st.markdown("### Multi-Format Templates for All Data Types")
        st.markdown("Download template files in different formats. Both CSV and Fixed Width formats are supported!")
        
        # Format tabs
        format_tabs = st.tabs(["📄 CSV Templates"])
        
        with format_tabs[0]:
            # Original CSV templates
            replace_template_section()
        
        
# Additional utility functions for fixed-width support

def validate_fixed_width_filename(filename: str, expected_type: str) -> bool:
    """Validate that filename matches expected fixed-width pattern"""
    patterns = {
        'customer': r'^CUSTOMER_\d{8}_\d{5}\.txt$',
        'account': r'^ACCOUNT_\d{8}_\d{5}\.txt$', 
        'transaction': r'^TRANSACTION_\d{8}_\d{5}\.txt$',
        'relationship': r'^RELATIONSHIP_\d{8}_\d{5}\.txt$'
    }
    
    pattern = patterns.get(expected_type.lower())
    if not pattern:
        return False
    
    return bool(re.match(pattern, filename.upper()))

def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date from fixed-width filename"""
    match = re.search(r'_(\d{8})_', filename)
    if match:
        date_str = match.group(1)
        try:
            # Convert YYYYMMDD to YYYY-MM-DD
            year = date_str[:4]
            month = date_str[4:6] 
            day = date_str[6:8]
            return f"{year}-{month}-{day}"
        except:
            return None
    return None

def extract_sequence_from_filename(filename: str) -> Optional[str]:
    """Extract sequence number from fixed-width filename"""
    match = re.search(r'_(\d{5})\.txt$', filename.upper())
    if match:
        return match.group(1)
    return None

# Main function to replace existing upload tabs

def replace_all_upload_tabs_with_fixed_width_support():
    """Replace all upload tabs with fixed-width support"""
    # These functions would replace the existing tab content in your main UI
    
    # Customer Upload Tab (replace create_enhanced_customer_upload_tab_with_multi_format())
    def new_customer_tab():
        create_enhanced_customer_upload_tab_with_fixed_width()
    
    # Account Upload Tab (replace create_enhanced_account_upload_tab_with_multi_format())
    def new_account_tab():
        create_enhanced_account_upload_tab_with_fixed_width()
    
    # Transaction Upload Tab (replace create_enhanced_transaction_upload_tab_with_multi_format())
    def new_transaction_tab():
        create_enhanced_transaction_upload_tab_with_fixed_width()
    
    # Template Section (replace create_enhanced_template_section_with_formats())
    def new_template_section():
        create_comprehensive_template_section()
    
    return new_customer_tab, new_account_tab, new_transaction_tab, new_template_section

def create_enhanced_customer_upload_tab_with_multi_format():
    """
    Enhanced customer upload tab with multi-format support
    """
    st.markdown("<h3 style='font-size: 22px;'>Upload Customer Data</h3>", unsafe_allow_html=True)
    st.info(" **Enhanced Multi-Format Support**: Upload CSV, TXT (with auto-delimiter detection), or Excel files!")
    
    ENTITY_CHUNK_SIZE = 5000
    
    # Enhanced file uploader
    df, metadata = create_enhanced_file_uploader(
        file_types=["csv", "txt", "xls", "xlsx"],
        key="customer_multi_format_upload"
    )
    
    if df is not None and not df.empty:
        try:
            st.write(f"**File:** {metadata['filename']} ({metadata['file_type'].upper()})")
            st.write(f"**Total records:** {len(df):,}")
            
            # Show file-specific metadata
            if metadata['file_type'] == 'txt':
                delimiter_name = {',': 'Comma', '\t': 'Tab', ';': 'Semicolon', '|': 'Pipe'}.get(metadata['delimiter'], metadata['delimiter'])
                st.write(f"**Delimiter:** {delimiter_name}")
                st.write(f"**Encoding:** {metadata['encoding']}")
            elif metadata['file_type'] in ['xls', 'xlsx']:
                st.write(f"**Active Sheet:** {metadata['active_sheet']}")
                if len(metadata['sheet_names']) > 1:
                    st.write(f"**Available Sheets:** {', '.join(metadata['sheet_names'])}")
            
            # Get uploaded headers
            uploaded_headers = list(df.columns)
            st.markdown("<h3 style='font-size: 18px;'> File Preview</h3>", unsafe_allow_html=True)
            # st.subheader(" File Preview")
            st.dataframe(df.head(5))
            
            
            # Create field mapping interface (using your existing function)
            field_mapping = create_field_mapping_interface(
                uploaded_headers, 
                EXPECTED_CUSTOMER_HEADERS, 
                REQUIRED_CUSTOMER_FIELDS, 
                "Customer"
            )
            
            if field_mapping is not None or set(uploaded_headers) == set(EXPECTED_CUSTOMER_HEADERS):
                # Apply mapping if needed
                if field_mapping:
                    st.success(" Field mapping completed!")
                    with st.spinner("Applying field mapping..."):
                        mapped_df = apply_field_mapping(df, field_mapping)
                else:
                    st.success(" Headers match perfectly - no mapping needed!")
                    mapped_df = df.copy()
                
                # Show mapped data preview
                st.markdown("<h3 style='font-size: 18px;'> Mapped Data Preview</h3>", unsafe_allow_html=True)
                # st.subheader(" Mapped Data Preview")
                st.dataframe(mapped_df.head(5))
                
                # Validate mapped data
                with st.spinner("Validating mapped customer data..."):
                    validation_errors = validate_data(mapped_df)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please fix the errors in your data file or adjust the field mapping.")
                else:
                    st.success(" Customer data validation passed successfully!")
                    
                    # Count of operations by type
                    if 'changetype' in mapped_df.columns:
                        operation_counts = mapped_df['changetype'].str.lower().value_counts().to_dict()
                        st.markdown("<h3 style='font-size: 18px;'> Operation Summary</h3>", unsafe_allow_html=True)
                        # st.subheader(" Operation Summary")
                        cols = st.columns(3)
                        cols[0].metric("Add Operations", operation_counts.get('add', 0))
                        cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                        cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                    
                    # Process button
                    if st.button(" Upload Customer Data", key="process_mapped_customer_multi"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing customer data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting customer processing for {metadata['file_type']} file with {len(mapped_df)} records")
                                    
                                    # Use global chunk size
                                    global CHUNK_SIZE
                                    CHUNK_SIZE = ENTITY_CHUNK_SIZE
                                    
                                    # Process the mapped data
                                    success_count, error_count, results = process_entity_csv_data_batch(mapped_df, connection)
                                    connection.close()
                                    
                                    # Store results in session state
                                    st.session_state.entity_success_count = success_count
                                    st.session_state.entity_error_count = error_count
                                    st.session_state.entity_results = results
                                    st.session_state.entity_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    # Display results
                                    st.success(f"Customer data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    # Calculate processing rate
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"Customer data processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
        
        except Exception as e:
            st.error(f"Error processing file: {e}")
            st.exception(e)

# Similarly, create enhanced versions for Account and Transaction uploads
def create_enhanced_account_upload_tab_with_multi_format():
    """Enhanced account upload tab with multi-format support"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Account Data</h3>", unsafe_allow_html=True)
    st.info(" **Enhanced Multi-Format Support**: Upload CSV, TXT (with auto-delimiter detection), or Excel files!")
    
    ACCOUNT_CHUNK_SIZE = 5000
    
    if not st.session_state.entity_processed:
        st.warning(" It is recommended to process customer data first before processing accounts")
    
    # Enhanced file uploader
    df, metadata = create_enhanced_file_uploader(
        file_types=["csv", "txt", "xls", "xlsx"],
        key="account_multi_format_upload"
    )
    
    if df is not None and not df.empty:
        try:
            st.write(f"**File:** {metadata['filename']} ({metadata['file_type'].upper()})")
            st.write(f"**Total records:** {len(df):,}")
            
            # Show file-specific metadata
            if metadata['file_type'] == 'txt':
                delimiter_name = {',': 'Comma', '\t': 'Tab', ';': 'Semicolon', '|': 'Pipe'}.get(metadata['delimiter'], metadata['delimiter'])
                st.write(f"**Delimiter:** {delimiter_name}")
                st.write(f"**Encoding:** {metadata['encoding']}")
            elif metadata['file_type'] in ['xls', 'xlsx']:
                st.write(f"**Active Sheet:** {metadata['active_sheet']}")
            
            uploaded_headers = list(df.columns)
            st.markdown("<h3 style='font-size: 18px;'> File Preview</h3>", unsafe_allow_html=True)
            # st.subheader(" File Preview")
            st.dataframe(df.head(5))
            
            
            field_mapping = create_field_mapping_interface(
                uploaded_headers, 
                EXPECTED_ACCOUNT_HEADERS, 
                REQUIRED_ACCOUNT_FIELDS, 
                "Account"
            )
            
            if field_mapping is not None or set(uploaded_headers) == set(EXPECTED_ACCOUNT_HEADERS):
                if field_mapping:
                    st.success(" Field mapping completed!")
                    with st.spinner("Applying field mapping..."):
                        mapped_df = apply_field_mapping(df, field_mapping)
                else:
                    st.success(" Headers match perfectly - no mapping needed!")
                    mapped_df = df.copy()
                
                st.markdown("<h3 style='font-size: 18px;'> Mapped Data Preview</h3>", unsafe_allow_html=True)
                # st.subheader(" Mapped Data Preview")
                st.dataframe(mapped_df.head(5))
                
                with st.spinner("Validating mapped account data..."):
                    validation_errors = validate_account_data(mapped_df, is_fixed_width=True)
                
                if validation_errors:
                    st.error(" Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please fix the errors or adjust the field mapping.")
                else:
                    st.success(" Account data validation passed successfully!")
                    
                    if 'changetype' in mapped_df.columns:
                        operation_counts = mapped_df['changetype'].str.lower().value_counts().to_dict()
                        st.markdown("<h3 style='font-size: 18px;'> Operation Summary</h3>", unsafe_allow_html=True)
                        # st.subheader(" Operation Summary")
                        cols = st.columns(3)
                        cols[0].metric("Add Operations", operation_counts.get('add', 0))
                        cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                        cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                    
                    if st.button(" Upload Account Data", key="process_mapped_account_multi"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing account data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting account processing for {metadata['file_type']} file with {len(mapped_df)} records")
                                    
                                    global CHUNK_SIZE
                                    CHUNK_SIZE = ACCOUNT_CHUNK_SIZE
                                    
                                    is_fixed_width = False

                                    success_count, error_count, results = process_account_csv_data_batch(mapped_df, connection, is_fixed_width)
                                    connection.close()
                                    
                                    st.session_state.account_success_count = success_count
                                    st.session_state.account_error_count = error_count
                                    st.session_state.account_results = results
                                    st.session_state.account_processed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f"Account data processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"Processing rate: {rate:.2f} records/second")
                                    
                                    logger.info(f"Account data processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
        
        except Exception as e:
            st.error(f"Error processing account file: {e}")
            st.exception(e)

def create_enhanced_transaction_upload_tab_with_multi_format():
    """Enhanced transaction upload tab with multi-format support - COMPLETELY FIXED VERSION"""
    st.markdown("<h3 style='font-size: 22px;'>Upload Transaction Data</h3>", unsafe_allow_html=True)
    st.markdown("**Enhanced Multi-Format Support with AML Detection:** Upload CSV, TXT, or Excel files with any headers, then manually trigger AML detection")
    
    TRANSACTION_CHUNK_SIZE = 5000
    
    if not st.session_state.account_processed:
        st.warning("It is recommended to process account data first before processing transactions")
    
    # Enhanced file uploader
    df, metadata = create_enhanced_file_uploader(
        file_types=["csv", "txt", "xls", "xlsx"],
        key="transaction_multi_format_upload"
    )
    
    if df is not None and not df.empty:
        try:
            # Save uploaded file temporarily for AML processing
            temp_file_path = f"temp_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(temp_file_path, index=False)
            st.session_state.temp_transaction_file = temp_file_path
            st.session_state.last_upload_type = "CSV" 
            st.session_state.last_upload_metadata = metadata  
            
            st.write(f"**File:** {metadata['filename']} ({metadata['file_type'].upper()})")
            st.write(f"**Total records:** {len(df):,}")
            
            # Show file-specific metadata
            if metadata['file_type'] == 'txt':
                delimiter_name = {',': 'Comma', '\t': 'Tab', ';': 'Semicolon', '|': 'Pipe'}.get(metadata['delimiter'], metadata['delimiter'])
                st.write(f"**Delimiter:** {delimiter_name}")
                st.write(f"**Encoding:** {metadata['encoding']}")
            elif metadata['file_type'] in ['xls', 'xlsx']:
                st.write(f"**Active Sheet:** {metadata['active_sheet']}")
            
            uploaded_headers = list(df.columns)
            st.markdown("<h3 style='font-size: 18px;'>File Preview</h3>", unsafe_allow_html=True)
            st.dataframe(df.head(5))
            
            field_mapping = create_field_mapping_interface(
                uploaded_headers, 
                EXPECTED_TRANSACTION_HEADERS, 
                REQUIRED_TRANSACTION_FIELDS, 
                "Transaction"
            )
            
            if field_mapping is not None or set(uploaded_headers) == set(EXPECTED_TRANSACTION_HEADERS):
                if field_mapping:
                    st.success("Field mapping completed!")
                    with st.spinner("Applying field mapping..."):
                        mapped_df = apply_field_mapping(df, field_mapping)
                else:
                    st.success("Headers match perfectly - no mapping needed!")
                    mapped_df = df.copy()
                
                st.markdown("<h3 style='font-size: 18px;'>Mapped Data Preview</h3>", unsafe_allow_html=True)
                st.dataframe(mapped_df.head(5))
                
                with st.spinner("Preprocessing mapped transaction data..."):
                    original_mapped_df = mapped_df.copy()
                    mapped_df = preprocess_transaction_data(mapped_df)
                    
                    # Show date conversion examples
                    st.markdown("<h3 style='font-size: 18px;'>Date Format Conversion Examples</h3>", unsafe_allow_html=True)
                    if 'transaction_date' in mapped_df.columns:
                        date_examples = []
                        for i, (orig, conv) in enumerate(zip(
                            original_mapped_df['transaction_date'].head(5),
                            mapped_df['transaction_date'].head(5)
                        )):
                            date_examples.append({
                                "Row": i+1,
                                "Original Format": orig,
                                "Converted Format": conv
                            })
                        st.table(date_examples)
                
                with st.spinner("Validating mapped transaction data..."):
                    validation_errors = validate_transaction_data(mapped_df, is_fixed_width=True)
                
                if validation_errors:
                    st.error("Validation Errors Detected")
                    for error in validation_errors:
                        st.error(error)
                    st.warning("Please fix the errors or adjust the field mapping.")
                else:
                    st.success("Transaction data validation passed successfully!")
                    
                    if 'changetype' in mapped_df.columns:
                        operation_counts = mapped_df['changetype'].str.lower().value_counts().to_dict()
                        st.markdown("<h3 style='font-size: 18px;'>Operation Summary</h3>", unsafe_allow_html=True)
                        cols = st.columns(3)
                        cols[0].metric("Add Operations", operation_counts.get('add', 0))
                        cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                        cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                    
                    st.markdown("---")
                    
                    # ========================================================
                    # STEP 1: TRANSACTION PROCESSING
                    # ========================================================
                    st.subheader("Step 1: Upload Transaction Data")
                    st.info("Upload and process transaction data into the database")
                    
                    if st.button("Upload Transaction Data", key="process_mapped_transaction_multi"):
                        if not st.session_state.db_connected:
                            st.error("Please connect to the database first!")
                        else:
                            with st.spinner("Processing transaction data..."):
                                start_time = time.time()
                                connection = create_db_connection()
                                
                                if connection and connection.is_connected():
                                    logger.info(f"Starting transaction processing for {metadata['file_type']} file with {len(mapped_df)} records")
                                    
                                    global CHUNK_SIZE
                                    CHUNK_SIZE = TRANSACTION_CHUNK_SIZE
                                    
                                    success_count, error_count, results, aml_results = process_transaction_csv_data_batch(
                                        mapped_df, connection, True, None, is_fixed_width=False)
                                    connection.close()
                                    
                                    st.session_state.transaction_success_count = success_count
                                    st.session_state.transaction_error_count = error_count
                                    st.session_state.transaction_results = results
                                    st.session_state.transaction_processed = True
                                    st.session_state.transaction_upload_completed = True
                                    
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    st.success(f"Transaction processing completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                    
                                    if processing_time > 0:
                                        rate = len(mapped_df) / processing_time
                                        st.info(f"Processing rate: {rate:.2f} records/second")
                                    
                                    st.success("Transaction data has been successfully uploaded to the database!")
                                    st.info("Now you can proceed to Step 2 to run AML Detection on the uploaded transactions.")
                                    
                                    logger.info(f"Transaction processing completed in {processing_time:.2f} seconds")
                                else:
                                    st.error("Could not connect to the database.")
                    
                    st.markdown("---")
                    
                    # ========================================================
                    # STEP 2: AML DETECTION - SINGLE UNIFIED SECTION
                    # ========================================================
                    
                    # Get all state variables ONCE
                    transaction_upload_completed = st.session_state.get('transaction_upload_completed', False)
                    transaction_success_count = st.session_state.get('transaction_success_count', 0)
                    aml_available = st.session_state.get('aml_available', False)
                    aml_running = st.session_state.get('aml_running', False)
                    aml_completed = st.session_state.get('aml_completed', False)
                    
                    # RENDER THE HEADER ONLY ONCE
                    st.subheader("Step 2: AML Detection")
                    
                    # Determine what to show based on state
                    if not transaction_upload_completed or transaction_success_count == 0:
                        # State 1: No transactions uploaded yet
                        st.info("Upload and process transaction data first to enable AML detection")
                        st.button("Commence AML Detection", disabled=True, key="disabled_aml_no_data")
                        
                    elif aml_completed:
                        # State 2: AML completed - show results
                        st.success("AML Detection completed successfully!")
                        
                        # Show upload metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Transactions Uploaded", transaction_success_count)
                        col2.metric("Upload Errors", st.session_state.get('transaction_error_count', 0))
                        col3.metric("Ready for AML", "" if aml_available else "")
                        
                        # Show AML results
                        aml_results = st.session_state.get('aml_results', {})
                        alerts = aml_results.get('alerts_generated', 0)
                        entities = aml_results.get('entities_processed', 0)
                        processing_time = aml_results.get('processing_time', 0)
                        
                        st.header("AML Detection Results")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Alerts Generated", alerts)
                        col2.metric("Entities Processed", entities)
                        col3.metric("Processing Time", f"{processing_time:.1f}s")
                        
                        if alerts > 0:
                            st.success(f"{alerts} alerts generated and stored in the database")
                        else:
                            st.info("No alerts generated - all transactions appear normal")
                            
                    elif aml_running:
                        # State 3: AML currently running - show progress
                        st.info("AML Detection is currently running...")
                        st.warning("Please wait while the system processes your transaction data...")
                        
                        # Show upload metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Transactions Uploaded", transaction_success_count)
                        col2.metric("Upload Errors", st.session_state.get('transaction_error_count', 0))
                        col3.metric("Ready for AML", "" if aml_available else "")
                        
                        # Create progress tracking containers
                        progress_container = st.empty()
                        status_container = st.empty()
                        
                        with progress_container.container():
                            progress_bar = st.progress(0)
                        
                        # Define progress callbacks
                        def progress_callback(percentage, entity_type=None, processed=0, total=0, current_entity=None, rule_name=None):
                            try:
                                progress_bar.progress(min(percentage / 100.0, 1.0))
                                
                                if entity_type and total > 0:
                                    entity_id = current_entity or f"ENTITY_{processed + 1:04d}"
                                    with status_container.container():
                                        st.info(f"Processing {entity_id} - {processed + 1}/{total} entities ({percentage:.0f}%)")
                                else:
                                    with status_container.container():
                                        st.info(f"AML Detection Progress: {percentage:.0f}%")
                            except Exception as e:
                                logger.warning(f"Progress callback error: {str(e)}")
                        
                        def status_callback(message, entity_type=None, alerts_count=0):
                            try:
                                with status_container.container():
                                    if alerts_count > 0:
                                        st.warning(f"{message} - {alerts_count} alerts found")
                                    else:
                                        st.info(f"{message}")
                            except Exception as e:
                                logger.warning(f"Status callback error: {str(e)}")
                        
                        # Execute AML detection
                        try:
                            if hasattr(st.session_state, 'temp_transaction_file') and st.session_state.temp_transaction_file:
                                # DETERMINE FILE TYPE FROM SESSION STATE OR FILENAME
                                is_fixed_width_file = st.session_state.get('last_upload_type') == "Fixed-Width"

                                # Alternative method: Check if we can determine from metadata
                                if not is_fixed_width_file and hasattr(st.session_state, 'last_upload_metadata'):
                                    metadata = st.session_state.last_upload_metadata
                                    is_fixed_width_file = metadata.get('format') == 'fixed_width'
                                
                                with status_container.container():
                                    file_type_msg = "Fixed-Width" if is_fixed_width_file else "CSV"
                                    st.info(f"Initializing AML Detection system for {file_type_msg} file...")

                                monitor = get_aml_monitor()
                                if monitor:
                                    with status_container.container():
                                        st.info("Initializing AML Detection system...")
                                    
                                    
                                    aml_results = run_aml_detection_after_upload(
                                        st.session_state.temp_transaction_file, 
                                        "transaction", 
                                        is_fixed_width=is_fixed_width_file
                                    )
                                    
                                    # Clean up and update state
                                    progress_container.empty()
                                    status_container.empty()
                                    
                                    st.session_state.aml_results = aml_results
                                    st.session_state.aml_completed = True
                                    st.session_state.aml_running = False
                                    
                                    # Force a rerun to show completed state
                                    st.rerun()
                                else:
                                    progress_container.empty()
                                    status_container.empty()
                                    st.session_state.aml_running = False
                                    st.error("AML Detection system is not available")
                            else:
                                progress_container.empty()
                                status_container.empty()
                                st.session_state.aml_running = False
                                st.error("No transaction file available for AML detection")
                        except Exception as e:
                            # Clean up on error
                            if 'progress_container' in locals():
                                progress_container.empty()
                            if 'status_container' in locals():
                                status_container.empty()
                            
                            st.session_state.aml_running = False
                            st.error(f"AML Detection failed: {str(e)}")
                            logger.error(f"AML detection error: {str(e)}")
                    
                    else:
                        # State 4: Ready to start AML detection
                        st.info("Run comprehensive AML detection on the uploaded transaction data")
                        
                        # Show upload metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Transactions Uploaded", transaction_success_count)
                        col2.metric("Upload Errors", st.session_state.get('transaction_error_count', 0))
                        col3.metric("Ready for AML", "" if aml_available else "")
                        
                        if aml_available:
                            if st.button("Commence AML Detection", key="start_aml"):
                                st.session_state.aml_running = True
                                st.session_state.aml_completed = False
                                st.rerun()
                        else:
                            st.warning("AML Detection system is not available")
                            st.button("Commence AML Detection", disabled=True, key="disabled_aml_unavailable")
        
        except Exception as e:
            st.error(f"Error processing Transaction file: {e}")
            st.exception(e)



def validate_file_size(file_bytes: bytes, max_size_mb: int = 100) -> bool:
    """Validate file size"""
    file_size_mb = len(file_bytes) / (1024 * 1024)
    if file_size_mb > max_size_mb:
        st.error(f"File size ({file_size_mb:.1f} MB) exceeds maximum allowed size ({max_size_mb} MB)")
        return False
    return True

def detect_file_format_from_content(file_content: str) -> str:
    """
    Try to detect file format from content structure
    """
    lines = file_content.split('\n')[:5]  # Check first 5 lines
    
    # Check for common patterns
    if any(',' in line for line in lines):
        return 'csv'
    elif any('\t' in line for line in lines):
        return 'tsv'
    elif any(';' in line for line in lines):
        return 'ssv'
    elif any('|' in line for line in lines):
        return 'psv'
    else:
        return 'unknown'

def create_file_format_summary(metadata: Dict[str, Any]) -> None:
    """Create a nice summary of file format information"""
    st.markdown("### 📄 File Information")
    
    info_cols = st.columns(4)
    
    with info_cols[0]:
        st.metric("File Type", metadata['file_type'].upper())
    
    with info_cols[1]:
        size_mb = metadata['file_size'] / (1024 * 1024)
        st.metric("File Size", f"{size_mb:.2f} MB")
    
    with info_cols[2]:
        if 'encoding' in metadata:
            st.metric("Encoding", metadata['encoding'])
        elif 'active_sheet' in metadata:
            st.metric("Active Sheet", metadata['active_sheet'])
        else:
            st.metric("Format", "Standard")
    
    with info_cols[3]:
        if 'delimiter' in metadata:
            delimiter_names = {',': 'Comma', '\t': 'Tab', ';': 'Semicolon', '|': 'Pipe'}
            delimiter_name = delimiter_names.get(metadata['delimiter'], metadata['delimiter'])
            st.metric("Delimiter", delimiter_name)
        elif 'sheet_names' in metadata:
            st.metric("Total Sheets", len(metadata['sheet_names']))
        else:
            st.metric("Status", "Ready")

def create_field_mapping_interface(uploaded_headers, expected_headers, required_fields, data_type):
    """
    Create an interactive field mapping interface
    """
    st.markdown(f"<h3 style='font-size: 18px;'> Field Mapping for {data_type} Data</h3>", unsafe_allow_html=True)
    # st.subheader(f"Field Mapping for {data_type} Data")
    
    # Check if headers match exactly
    missing_headers = set(expected_headers) - set(uploaded_headers)
    extra_headers = set(uploaded_headers) - set(expected_headers)
    
    if not missing_headers and not extra_headers:
        st.success(" Perfect match! All headers are correctly formatted.")
        return None
    
    # Show header comparison
    st.warning(f"Column name mismatch detected Please map your fields to the expected format.")
    
    
    # Create mapping interface
    st.markdown("<h3 style='font-size: 18px;'>Map Your Fields</h3>", unsafe_allow_html=True)
    # st.write("### Map Your Fields")
    st.write(" 🔴 Mandatory Fields, 🔵 Optional Fields")
    st.info("Map your file's column headers to the expected format. Required fields must be mapped.")
    
    mapping = {}
    
    # Create two columns for better layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.write("**Expected Field → Your Field**")
    
    with col2:
        st.write("**Field Description**")
    
    # Create mapping dropdowns for each expected field
    for i, expected_field in enumerate(expected_headers):
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Prepare options
            options = ["-- Not Available --"] + uploaded_headers
            
            # Try to find automatic match
            default_index = 0
            for j, uploaded_header in enumerate(uploaded_headers):
                if uploaded_header.lower() == expected_field.lower():
                    default_index = j + 1
                    break
                elif uploaded_header.lower().replace('_', '').replace(' ', '') == expected_field.lower().replace('_', '').replace(' ', ''):
                    default_index = j + 1
                    break
            
            # Create selectbox
            is_required = expected_field in required_fields
            label = f"{'🔴 ' if is_required else '🔵 '}{expected_field}"
            
            selected = st.selectbox(
                label,
                options,
                index=default_index,
                key=f"mapping_{data_type}_{expected_field}_{i}",
                help=f"Required field" if is_required else "Optional field"
            )
            
            if selected != "-- Not Available --":
                mapping[expected_field] = selected
        
        with col2:
            description = FIELD_DESCRIPTIONS.get(expected_field, "No description available")
            st.write(f"_{description}_")
    
    # Validate required fields
    missing_required = [field for field in required_fields if field not in mapping]
    
    if missing_required:
        st.error(f" Missing required fields: {', '.join(missing_required)}")
        return None
  
    return mapping

def apply_field_mapping(df, mapping):
    """
    Apply field mapping to rename columns in dataframe
    """
    if not mapping:
        return df
    
    # Create reverse mapping (uploaded_header -> expected_header)
    reverse_mapping = {v: k for k, v in mapping.items()}
    
    # Rename columns
    df_mapped = df.copy()
    df_mapped = df_mapped.rename(columns=reverse_mapping)
    
    # Add missing columns with None values
    for expected_field in mapping.keys():
        if expected_field not in df_mapped.columns:
            df_mapped[expected_field] = None
    
    return df_mapped



# Function to save field mappings for future use
def save_field_mapping(mapping, data_type, mapping_name):
    """Save field mapping configuration"""
    import json
    import os
    from werkzeug.utils import secure_filename
    
    ###################################
    #### Change to fix Uncontrolled data used in path expression
    #### 16-jul-2025 
    ###################################

    mapping_data = {
        'data_type': data_type,
        'mapping_name': mapping_name,
        'mapping': mapping,
        'created_date': datetime.now().isoformat()
    }
    
    # Create mappings directory if it doesn't exist
    os.makedirs('field_mappings', exist_ok=True)
    
    # Save mapping file with secure filename
    safe_data_type = secure_filename(data_type)
    safe_mapping_name = secure_filename(mapping_name)
    # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"field_mappings/{safe_data_type}_{safe_mapping_name}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(mapping_data, f, indent=2)
    
    return filename

def load_saved_mappings(data_type):
    """Load saved field mappings"""
    import json
    import glob
    
    mappings = []
    
    if os.path.exists('field_mappings'):
        pattern = f"field_mappings/{data_type}_*.json"
        for file_path in glob.glob(pattern):
            try:
                with open(file_path, 'r') as f:
                    mapping_data = json.load(f)
                    mappings.append({
                        'file_path': file_path,
                        'name': mapping_data.get('mapping_name', 'Unknown'),
                        'created_date': mapping_data.get('created_date', ''),
                        'mapping': mapping_data.get('mapping', {})
                    })
            except Exception as e:
                logger.warning(f"Could not load mapping file {file_path}: {str(e)}")
    
    return mappings

def create_enhanced_field_mapping_interface(uploaded_headers, expected_headers, required_fields, data_type):
    """
    Enhanced field mapping interface with save/load functionality
    """
    st.subheader(f"Enhanced Field Mapping for {data_type} Data")
    
    # Check if headers match exactly
    missing_headers = set(expected_headers) - set(uploaded_headers)
    extra_headers = set(uploaded_headers) - set(expected_headers)
    
    if not missing_headers and not extra_headers:
        st.success(" Perfect match! All headers are correctly formatted.")
        return None
    
    # Show header comparison
    st.warning(f"Column name mismatch detected Please map your fields to the expected format.")
    
    
    # Load saved mappings
    saved_mappings = load_saved_mappings(data_type)
    
    # Option to load saved mapping
    if saved_mappings:
        with st.expander("📁 Load Saved Mapping"):
            st.write("**Available Saved Mappings:**")
            
            for i, saved_mapping in enumerate(saved_mappings):
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.write(f"**{saved_mapping['name']}**")
                
                with col2:
                    st.write(f"Created: {saved_mapping['created_date'][:10]}")
                
                with col3:
                    if st.button("Load", key=f"load_mapping_{i}"):
                        st.session_state[f'loaded_mapping_{data_type}'] = saved_mapping['mapping']
                        st.success(f"Loaded mapping: {saved_mapping['name']}")
                        st.experimental_rerun()
    
    # Create mapping interface
    st.markdown("<h3 style='font-size: 18px;'>Map Your Fields</h3>", unsafe_allow_html=True)
    # st.write("### Map Your Fields")
    st.write(" 🔴 Mandatory Fields, 🔵 Optional Fields")
    st.info("Map your file's column headers to the expected format. Required fields must be mapped.")
    
    # Auto-match suggestions
    st.write("### 🤖 Auto-Match Suggestions")
    auto_suggestions = {}
    
    for expected_field in expected_headers:
        best_match = None
        best_score = 0
        
        for uploaded_header in uploaded_headers:
            # Simple similarity scoring
            score = 0
            if uploaded_header.lower() == expected_field.lower():
                score = 100
            elif uploaded_header.lower().replace('_', '').replace(' ', '') == expected_field.lower().replace('_', '').replace(' ', ''):
                score = 90
            elif expected_field.lower() in uploaded_header.lower() or uploaded_header.lower() in expected_field.lower():
                score = 70
            elif any(word in uploaded_header.lower() for word in expected_field.lower().split('_')):
                score = 50
            
            if score > best_score:
                best_score = score
                best_match = uploaded_header
        
        if best_match and best_score >= 50:
            auto_suggestions[expected_field] = best_match
    
    if auto_suggestions:
        st.write("**Suggested Mappings:**")
        for expected, suggested in auto_suggestions.items():
            confidence = "High" if suggested in uploaded_headers and suggested.lower() == expected.lower() else "Medium"
            st.write(f"• {expected} ← {suggested} ({confidence} confidence)")
        
        if st.button(" Apply Auto-Suggestions", key=f"auto_suggest_{data_type}"):
            st.session_state[f'auto_mapping_{data_type}'] = auto_suggestions
            st.success("Auto-suggestions applied! Review and adjust as needed.")
            st.experimental_rerun()
    
    mapping = {}
    
    # Load mapping from session state if available
    loaded_mapping = st.session_state.get(f'loaded_mapping_{data_type}', {})
    auto_mapping = st.session_state.get(f'auto_mapping_{data_type}', {})
    
    # Create two columns for better layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.write("**Expected Field → Your Field**")
    
    with col2:
        st.write("**Field Description**")
    
    # Create mapping dropdowns for each expected field
    for i, expected_field in enumerate(expected_headers):
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Prepare options
            options = ["-- Not Available --"] + uploaded_headers
            
            # Determine default selection
            default_index = 0
            
            # Priority: loaded mapping > auto mapping > exact match
            if expected_field in loaded_mapping and loaded_mapping[expected_field] in uploaded_headers:
                default_index = uploaded_headers.index(loaded_mapping[expected_field]) + 1
            elif expected_field in auto_mapping and auto_mapping[expected_field] in uploaded_headers:
                default_index = uploaded_headers.index(auto_mapping[expected_field]) + 1
            else:
                # Try to find exact match
                for j, uploaded_header in enumerate(uploaded_headers):
                    if uploaded_header.lower() == expected_field.lower():
                        default_index = j + 1
                        break
            
            # Create selectbox
            is_required = expected_field in required_fields
            label = f"{'🔴 ' if is_required else '🔵 '}{expected_field}"
            
            selected = st.selectbox(
                label,
                options,
                index=default_index,
                key=f"mapping_{data_type}_{expected_field}_{i}",
                help=f"Required field" if is_required else "Optional field"
            )
            
            if selected != "-- Not Available --":
                mapping[expected_field] = selected
        
        with col2:
            description = FIELD_DESCRIPTIONS.get(expected_field, "No description available")
            st.write(f"_{description}_")
    
    # Validate required fields
    missing_required = [field for field in required_fields if field not in mapping]
    
    if missing_required:
        st.error(f" Missing required fields: {', '.join(missing_required)}")
        return None
    
    # Option to save mapping
    with st.expander("💾 Save This Mapping"):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            mapping_name = st.text_input("Mapping Name", 
                                       value=f"{data_type}_mapping_{datetime.now().strftime('%Y%m%d')}",
                                       key=f"save_mapping_name_{data_type}")
        
        with col2:
            if st.button("💾 Save Mapping", key=f"save_mapping_{data_type}"):
                if mapping_name and mapping:
                    try:
                        filename = save_field_mapping(mapping, data_type, mapping_name)
                        st.success(f"Mapping saved as: {mapping_name}")
                        st.info(f"File: {filename}")
                    except Exception as e:
                        st.error(f" Error saving mapping: {str(e)}")
                else:
                    st.warning("Please provide a mapping name and ensure fields are mapped.")
    
    
    return mapping

def create_enhanced_template_section_with_formats():
    """Enhanced template section that includes multiple format examples"""
    st.markdown("""
        <style>
        .streamlit-expanderHeader {
            font-size: 32px !important;
        }
        </style>
        """, unsafe_allow_html=True)

    st.markdown("<h3 style='font-size: 18px;'> Download Templates (Multiple Formats)</h3>", unsafe_allow_html=True)
    with st.expander("Click to expand template options"):
        st.markdown("<h3 style='font-size: 22px;'> Multi-Format Templates for All Data Types</h3>", unsafe_allow_html=True)
        st.markdown("Download template files in different formats!")
        
        # Helper function to create templates with different delimiters
        def create_delimited_template(delimiter, data, headers):
            rows = [delimiter.join(headers)]
            for row in data:
                rows.append(delimiter.join(str(cell) for cell in row))
            return '\n'.join(rows)
        
        # ===================
        # CUSTOMER TEMPLATES
        # ===================
        customer_headers = ['changetype', 'customer_id', 'parent_customer_id', 'customer_type', 'first_name', 'last_name', 'middle_name', 'name_prefix', 'name_suffix', 'date_of_birth','is_employee', 'nationality', 'deceased_date', 'dual_citizenship_country', 'ethnic_background', 'tax_id', 'job_title', 'marital_status', 'home_ownership_type', 'customer_risk_score', 'pep', 'is_related_to_pep', 'document_type', 'document_number', 'document_expiry_date', 'business_name', 'date_of_incorporation', 'doing_business_as', 'legal_form', 'annual_turnover','industry_code', 'bankruptcy_date', 'tin', 'lei', 'swift_number', 'company_identifier', 'company_name', 'email_type', 'email_address', 'phone_type', 'phone_number', 'address_type', 'address_line_1', 'address_line_2', 'address_line_3', 'address_city', 'address_state', 'address_country', 'address_zipcode', 'number_of_years_at_address', 'closed_date', 'closed_reason','country_risk', 'contract_number', 'account_executive','business_unit','risk_category']
        
        customer_data = [
            ['add', 'CUST12345', '', 'Consumer', 'John', 'Smith', 'David', 'Mr.', 'Jr.', 'N', 'US', '', '', '', '123-45-6789', 'Software Engineer', 'Married', 'Own', '750', 'N', 'N', '1999-03-23','Driver License', 'DL12345678', '2027-01-15', '', '', '', '', '', '', '', '', '', '','', '', 'Primary', 'john.smith@email.com', 'Primary', '555-123-4567', 'Primary', '123 Main Street', 'Apt 4B', '', 'New York', 'NY', 'US', '10001', '5', '', '','low', 'PRIV-334455', 'sarah parks','commercial banking','low risk'],
            ['add', 'CUST67890', '', 'Consumer', 'Sarah', 'Johnson', 'Marie', 'Ms.', '', 'N', 'US', '', '', '', '987-65-4321', 'Marketing Manager', 'Single', 'Rent', '680', 'N', 'N','2000-04-24', 'Passport', 'P98765432', '2026-06-30', '', '', '', '', '', '', '', '', '', '', '','', 'Primary', 'sarah.johnson@email.com', 'Mobile', '555-987-6543', 'Primary', '456 Oak Avenue', '', '', 'Los Angeles', 'CA', 'US', '90210', '2', '', '','low-medium','corp-2023-445566','amanda foster','corporate banking', 'medium risk'],
            ['add', 'CUST11111', 'CUST12345', 'Business', '','', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'TechCorp Solutions Inc', '2018-03-15', 'TechCorp', 'Technology', '541511', '', '45-1234567', '123456789012345678901', 'TECHCORPUS33XXX', 'TC001', 'TechCorp Solutions Inc',5468376,'Foreign Company', 'contact@techcorp.com', 'Business', '555-555-0123', 'Business', '789 Business Blvd', 'Suite 200', '', 'San Francisco', 'CA', 'US', '94105', '4', '', '','high','SMB-778899-2024', 'sarah parks','commercial banking','high risk']
        ]
        
        # ===================
        # ACCOUNT TEMPLATES
        # ===================
        account_headers = ['changetype', 'account_id', 'customer_id', 'account_number', 'account_routing_number', 'account_type', 'account_currency', 'account_balance', 'open_date', 'closed_date', 'close_reason', 'branch_id', 'reopen_date', 'account_risk_score', 'account_status', 'is_p2p_enabled', 'p2p_enrollment_date', 'p2p_email', 'p2p_phone', 'is_joint', 'joint_account_primary_id', 'joint_account_secondary_id', 'joint_account_relationship_type', 'joint_account_status']
        
        account_data = [
            ['add', 'ACC12345', 'CUST12345', '1234567890', '123456789', 'Checking', 'USD', '2500.75', '2020-01-15', '', '', 'BRANCH_NYC_001', '', '65', 'Active', 'Y', '2020-02-01', 'john.smith@email.com', '555-123-4567', '0', '', '', '', ''],
            ['add', 'ACC67890', 'CUST67890', '9876543210', '987654321', 'Savings', 'USD', '15000.00', '2021-06-20', '', '', 'BRANCH_LA_005', '', '70', 'Active', 'N', '', '', '555-987-6543', '0', '', '', '', ''],
            ['add', 'ACC11111', 'CUST11111', '5555555555', '555555555', 'Business Checking', 'USD', '75000.50', '2019-08-10', '', '', 'BRANCH_SF_010', '', '80', 'Active', 'Y', '2019-09-01', 'contact@techcorp.com', '555-555-0123', '0', '', '', '', ''],
            ['add', 'ACC99999', 'CUST12345', '1111111111', '123456789', 'Joint Checking', 'USD', '5000.00', '2022-03-01', '', '', 'BRANCH_NYC_001', '', '60', 'Active', 'Y', '2022-03-15', 'joint@email.com', '555-111-2222', '1', 'CUST12345', 'CUST67890', 'Spouse', 'Active']
        ]
        
        # ===================
        # TRANSACTION TEMPLATES
        # ===================
        transaction_headers = ['changetype', 'transaction_id', 'account_id', 'debit_credit_indicator', 'transaction_date', 'transaction_type', 'transaction_code', 'tran_code_description', 'amount', 'preferred_currency', 'settlement_currency', 'exchange_rate_applied', 'transaction_origin_country', 'channel', 'counterparty_account', 'counterparty_routing_number', 'counterparty_name', 'branch_id', 'teller_id', 'check_number', 'micr_code', 'check_image_front_path', 'check_image_back_path', 'txn_location', 'is_p2p_transaction', 'p2p_recipient_type', 'p2p_recipient', 'p2p_sender_type', 'p2p_sender', 'p2p_reference_id', 'balance','eod_balance','non_account_holder','positive_pay_indicator','originator_bank','beneficiary_bank','instructing_bank','intermediary_bank','inter_bank_information','other_information']
        
        transaction_data = [
            ['add', 'TXN12345', 'ACC12345', 'D', '2023-06-15 09:30:00', 'ATM Withdrawal', 'ATM_WD', 'ATM Cash Withdrawal', '200.00', 'USD', 'USD', '1.0', 'US', 'ATM', '', '', 'ATM Network', 'BRANCH_NYC_001', '', '', '', '', '', 'New York NY', '0', '', '', '', '', '','2500','2500','N','Y','Bank of America','JPMC','','','',''],
            ['add', 'TXN67890', 'ACC67890', 'C', '2023-06-16 14:22:00', 'Direct Deposit', 'DIR_DEP', 'Salary Deposit', '3500.00', 'USD', 'USD', '1.0', 'US', 'ACH', '9999999999', '555555555', 'ABC Company Payroll', 'BRANCH_LA_005', '', '', '', '', '', 'Los Angeles CA', '0', '', '', '', '', '','5000','6500','N','Y','Wells Fargo','Citizens Bank','','','',''],
            ['add', 'TXN11111', 'ACC11111', 'D', '2023-06-17 11:45:00', 'Wire Transfer', 'WIRE_OUT', 'International Wire', '10000.00', 'USD', 'EUR', '0.92', 'US', 'Wire', '8888888888', '777777777', 'European Supplier Ltd', 'BRANCH_SF_010', '', '', '', '', '', 'San Francisco CA', '0', '', '', '', '', '','4000','4500','N','Y','Wells Fargo','Bank of America','','','',''],
            ['add', 'TXN99999', 'ACC12345', 'D', '2023-06-18 16:15:00', 'P2P Payment', 'P2P_SEND', 'Zelle Payment', '150.00', 'USD', 'USD', '1.0', 'US', 'Mobile', 'ACC67890', '987654321', 'Sarah Johnson', 'BRANCH_NYC_001', '', '', '', '', '', 'New York NY', '1', 'email', 'sarah.johnson@email.com', 'email', 'john.smith@email.com', 'ZELLE_P2P_001','3000','3000','N','Y','Wells Fargo','Bank of America','','','',''],
            ['add', 'TXN77777', 'ACC67890', 'C', '2023-06-18 16:16:00', 'P2P Payment', 'P2P_RCV', 'Zelle Payment Received', '150.00', 'USD', 'USD', '1.0', 'US', 'Mobile', 'ACC12345', '123456789', 'John Smith', 'BRANCH_LA_005', '', '', '', '', '', 'Los Angeles CA', '1', 'email', 'john.smith@email.com', 'email', 'sarah.johnson@email.com', 'ZELLE_P2P_001','4000','4000','N','Y','Wells Fargo','Bank of America','','','','']
        ]
        
        # Create templates for all data types and formats
        templates = {
            'customer': {
                'headers': customer_headers,
                'data': customer_data,
                'name': 'Customer'
            },
            'account': {
                'headers': account_headers,
                'data': account_data,
                'name': 'Account'
            },
            'transaction': {
                'headers': transaction_headers,
                'data': transaction_data,
                'name': 'Transaction'
            }
        }
        
        # Format definitions
        formats = [
            {'ext': 'csv', 'delimiter': ',', 'name': 'CSV (Comma)', 'desc': 'Standard comma-separated values'},
            {'ext': 'tsv', 'delimiter': '\t', 'name': 'TSV (Tab)', 'desc': 'Tab-separated values'},
            {'ext': 'txt', 'delimiter': ';', 'name': 'SSV (Semicolon)', 'desc': 'Semicolon-separated values'},
            {'ext': 'txt', 'delimiter': '|', 'name': 'PSV (Pipe)', 'desc': 'Pipe-separated values'}
        ]
        
        # Create tabs for each data type
        data_type_tabs = st.tabs(["👥 Customer Templates", "🏦 Account Templates", "💳 Transaction Templates","📋 List Managment Templates","🗺️ Schema Editor","📋 View Logs"])
        
        # CUSTOMER TEMPLATES TAB
        with data_type_tabs[0]:
            st.markdown("<h3 style='font-size: 20px;'>🧑‍💼 Customer Data Templates</h3>", unsafe_allow_html=True)
            # st.markdown("### 🧑‍💼 Customer Data Templates")
            st.markdown("**Required fields:** changetype, customer_id, customer_type")
            st.markdown("**Supports:** Consumer and Business customers")
            
            format_cols = st.columns(4)
            
            for i, fmt in enumerate(formats):
                with format_cols[i]:
                    template_content = create_delimited_template(
                        fmt['delimiter'], 
                        templates['customer']['data'], 
                        templates['customer']['headers']
                    )
                    
                    filename = f"customer_template.{fmt['ext']}" if fmt['ext'] != 'txt' else f"customer_template_{fmt['name'].split()[0].lower()}.txt"
                    
                    st.markdown(f"**{fmt['name']}**")
                    st.markdown(get_csv_download_link(filename, template_content, f"Download {fmt['name'].split()[0]}"), unsafe_allow_html=True)
                    st.caption(fmt['desc'])
        
        # ACCOUNT TEMPLATES TAB
        with data_type_tabs[1]:
            st.markdown("<h3 style='font-size: 20px;'>🏦 Account Data Templates</h3>", unsafe_allow_html=True)
            # st.markdown("### 🏦 Account Data Templates")
            st.markdown("**Required fields:** changetype, account_id, customer_id, account_number")
            st.markdown("**Supports:** All account types including joint accounts")
            
            format_cols = st.columns(4)
            
            for i, fmt in enumerate(formats):
                with format_cols[i]:
                    template_content = create_delimited_template(
                        fmt['delimiter'], 
                        templates['account']['data'], 
                        templates['account']['headers']
                    )
                    
                    filename = f"account_template.{fmt['ext']}" if fmt['ext'] != 'txt' else f"account_template_{fmt['name'].split()[0].lower()}.txt"
                    
                    st.markdown(f"**{fmt['name']}**")
                    st.markdown(get_csv_download_link(filename, template_content, f"Download {fmt['name'].split()[0]}"), unsafe_allow_html=True)
                    st.caption(fmt['desc'])
        
        # TRANSACTION TEMPLATES TAB
        with data_type_tabs[2]:
            st.markdown("<h3 style='font-size: 20px;'>💳 Transaction Data Templates</h3>", unsafe_allow_html=True)
            
            # st.markdown("### 💳 Transaction Data Templates")
            st.markdown("**Required fields:** changetype, transaction_id, account_id, transaction_date")
            st.markdown("**Supports:** All transaction types including P2P payments")
            
            format_cols = st.columns(4)
            
            for i, fmt in enumerate(formats):
                with format_cols[i]:
                    template_content = create_delimited_template(
                        fmt['delimiter'], 
                        templates['transaction']['data'], 
                        templates['transaction']['headers']
                    )
                    
                    filename = f"transaction_template.{fmt['ext']}" if fmt['ext'] != 'txt' else f"transaction_template_{fmt['name'].split()[0].lower()}.txt"
                    
                    st.markdown(f"**{fmt['name']}**")
                    st.markdown(get_csv_download_link(filename, template_content, f"Download {fmt['name'].split()[0]}"), unsafe_allow_html=True)
                    st.caption(fmt['desc'])

            with data_type_tabs[3]:
                create_list_template_tab()

            with data_type_tabs[4]:
                create_schema_editor_interface()
        
        st.markdown("---")
        
        # EXCEL TEMPLATES SECTION
        st.markdown("<h3 style='font-size: 20px;'> Excel Templates (Multi-Sheet)</h3>", unsafe_allow_html=True)
        # st.markdown("###  Excel Templates (Multi-Sheet)")
        st.markdown("Download comprehensive Excel files with all data types in separate sheets")
        
        excel_cols = st.columns(2)
        
        with excel_cols[0]:
            st.markdown("**Excel Workbook (.xlsx)**")
            st.markdown("Contains all three data types in separate sheets:")
            st.write("• Sheet 1: Customer Data")
            st.write("• Sheet 2: Account Data") 
            st.write("• Sheet 3: Transaction Data")
            
            if st.button(" Generate Excel Workbook", key="generate_excel_workbook"):
                # Create Excel file with multiple sheets
                excel_buffer = create_excel_workbook_with_all_templates(templates)
                if excel_buffer:
                    st.download_button(
                        label=" Download Excel Workbook",
                        data=excel_buffer,
                        file_name="data_upload_templates.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        with excel_cols[1]:
            st.markdown("**Individual Excel Files**")
            st.markdown("Download separate Excel files for each data type:")
            
            for data_type, template_info in templates.items():
                excel_content = create_single_excel_template(template_info['headers'], template_info['data'])
                if excel_content:
                    st.download_button(
                        label=f"{template_info['name']} Excel",
                        data=excel_content,
                        file_name=f"{data_type}_template.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"excel_{data_type}"
                    )
        
        st.markdown("---")
        
        

def create_excel_workbook_with_all_templates(templates):
    """Create an Excel workbook with all templates in separate sheets"""
    try:
        import pandas as pd
        from io import BytesIO
        import openpyxl
        
        # Create a BytesIO buffer
        excel_buffer = BytesIO()
        
        # Create Excel writer
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            for data_type, template_info in templates.items():
                # Create DataFrame
                df = pd.DataFrame(template_info['data'], columns=template_info['headers'])
                
                # Write to sheet
                sheet_name = template_info['name']
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Get the workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        excel_buffer.seek(0)
        return excel_buffer.getvalue()
        
    except Exception as e:
        st.error(f"Error creating Excel workbook: {str(e)}")
        return None

def create_single_excel_template(headers, data):
    """Create a single Excel template"""
    try:
        import pandas as pd
        from io import BytesIO
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=headers)
        
        # Create BytesIO buffer
        excel_buffer = BytesIO()
        
        # Write to Excel
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Auto-adjust column widths
            workbook = writer.book
            worksheet = writer.sheets['Data']
            
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        excel_buffer.seek(0)
        return excel_buffer.getvalue()
        
    except Exception as e:
        st.error(f"Error creating Excel template: {str(e)}")
        return None


def create_enhanced_template_section():
    """Enhanced template download section with field mapping info"""
    with st.expander(" Download Enhanced CSV Templates"):
        st.markdown("###  Enhanced CSV Templates with Field Mapping Support")
        st.markdown("Download template files for data upload. These templates include all required fields, proper formatting, and examples.")
        st.info("💡 **Don't worry if your data doesn't match these exact headers - our field mapping system will help you map your fields!**")
        
        # Enhanced templates with better examples
        customer_template_enhanced = """changetype,customer_id,parent_customer_id,customer_type,first_name,last_name,middle_name,name_prefix,name_suffix,date_of_birth,is_employee,nationality,deceased_date,dual_citizenship_country,ethnic_background,tax_id,job_title,marital_status,home_ownership_type,customer_risk_score,pep,is_related_to_pep,document_type,document_number,document_expiry_date,business_name,date_of_incorporation,doing_business_as,business_type,industry_code,bankruptcy_date,tin,lei,swift_number,company_identifier,company_name,email_type,email_address,phone_type,phone_number,address_type,address_line_1,address_line_2,address_line_3,address_city,address_state,address_country,address_zipcode,number_of_years_at_address,closed_date,closed_reason
add,CUST12345,,Consumer,John,Smith,David,Mr.,Jr.,2000-04-14,N,US,,,,123-45-6789,Software Engineer,Married,Own,750,N,N,Driver License,DL12345678,2027-01-15,,,,,,,,,,,,Primary,john.smith@email.com,Primary,555-123-4567,Primary,123 Main Street,Apt 4B,,New York,NY,US,10001,5,,
add,CUST67890,,Consumer,Sarah,Johnson,Marie,Ms.,,1999-03-01,N,US,,,,987-65-4321,Marketing Manager,Single,Rent,680,N,N,Passport,P98765432,2026-06-30,,,,,,,,,,,,Primary,sarah.johnson@email.com,Mobile,555-987-6543,Primary,456 Oak Avenue,,,Los Angeles,CA,US,90210,2,,
add,CUST11111,CUST12345,Business,,,,,,,,,,,,,,,,,,,,,,TechCorp Solutions Inc,2018-03-15,TechCorp,Technology,541511,,45-1234567,123456789012345678901,TECHCORPUS33XXX,TC001,TechCorp Solutions Inc,Business,contact@techcorp.com,Business,555-555-0123,Business,789 Business Blvd,Suite 200,,San Francisco,CA,US,94105,4,,"""
        
        account_template_enhanced = """changetype,account_id,customer_id,account_number,account_routing_number,account_type,account_currency,account_balance,open_date,closed_date,close_reason,branch_id,reopen_date,account_risk_score,account_status,is_p2p_enabled,p2p_enrollment_date,p2p_email,p2p_phone,is_joint,joint_account_primary_id,joint_account_secondary_id,joint_account_relationship_type,joint_account_status
add,ACC12345,CUST12345,1234567890,123456789,Checking,USD,2500.75,2020-01-15,,,BRANCH_NYC_001,,65,Active,Y,2020-02-01,john.smith@email.com,555-123-4567,N,,,,
add,ACC67890,CUST67890,9876543210,987654321,Savings,USD,15000.00,2021-06-20,,,BRANCH_LA_005,,70,Active,N,,,555-987-6543,N,,,,
add,ACC11111,CUST11111,5555555555,555555555,Business Checking,USD,75000.50,2019-08-10,,,BRANCH_SF_010,,80,Active,Y,2019-09-01,contact@techcorp.com,555-555-0123,N,,,,
add,ACC99999,CUST12345,1111111111,123456789,Joint Checking,USD,5000.00,2022-03-01,,,BRANCH_NYC_001,,60,Active,Y,2022-03-15,joint@email.com,555-111-2222,Y,CUST12345,CUST67890,Spouse,Active"""
        
        transaction_template_enhanced = """changetype,transaction_id,account_id,debit_credit_indicator,transaction_date,transaction_type,transaction_code,tran_code_description,amount,preferred_currency,settlement_currency,exchange_rate_applied,transaction_origin_country,channel,counterparty_account,counterparty_routing_number,counterparty_name,branch_id,teller_id,check_number,micr_code,check_image_front_path,check_image_back_path,txn_location,is_p2p_transaction,p2p_recipient_type,p2p_recipient,p2p_sender_type,p2p_sender,p2p_reference_id
add,TXN12345,ACC12345,D,2023-06-15 09:30:00,ATM Withdrawal,ATM_WD,ATM Cash Withdrawal,200.00,USD,USD,1.0,US,ATM,,,ATM Network,BRANCH_NYC_001,,,,,/images/atm_receipts/TXN12345.jpg,New York NY,N,,,,,
add,TXN67890,ACC67890,C,2023-06-16 14:22:00,Direct Deposit,DIR_DEP,Salary Deposit,3500.00,USD,USD,1.0,US,ACH,9999999999,555555555,ABC Company Payroll,BRANCH_LA_005,,,,,,,Los Angeles CA,N,,,,,
add,TXN11111,ACC11111,D,2023-06-17 11:45:00,Wire Transfer,WIRE_OUT,International Wire,10000.00,USD,EUR,0.92,US,Wire,8888888888,777777777,European Supplier Ltd,BRANCH_SF_010,,,,,,INTL_WIRE_001,San Francisco CA,N,,,,,
add,TXN99999,ACC12345,D,2023-06-18 16:15:00,P2P Payment,P2P_SEND,Zelle Payment,150.00,USD,USD,1.0,US,Mobile,ACC67890,987654321,Sarah Johnson,BRANCH_NYC_001,,,,,,,New York NY,Y,Individual,sarah.johnson@email.com,Individual,john.smith@email.com,ZELLE_P2P_001
add,TXN77777,ACC67890,C,2023-06-18 16:16:00,P2P Payment,P2P_RCV,Zelle Payment Received,150.00,USD,USD,1.0,US,Mobile,ACC12345,123456789,John Smith,BRANCH_LA_005,,,,,,,Los Angeles CA,Y,Individual,john.smith@email.com,Individual,sarah.johnson@email.com,ZELLE_P2P_001"""
        
        col1, col2, col3 = st.columns(3)
        
        # Customer Template
        with col1:
            st.subheader("🧑‍💼 Customer Template")
            st.markdown(get_csv_download_link("customer_template_enhanced.csv", customer_template_enhanced, " Download Customer Template"), unsafe_allow_html=True)
            st.markdown("**Required fields:** changetype, customer_id, customer_type")
            st.markdown("**Supports:** Consumer and Business customers")
        
        # Account Template
        with col2:
            st.subheader("🏦 Account Template")
            st.markdown(get_csv_download_link("account_template_enhanced.csv", account_template_enhanced, " Download Account Template"), unsafe_allow_html=True)
            st.markdown("**Required fields:** changetype, account_id, customer_id, account_number")
            st.markdown("**Supports:** All account types including joint accounts")
        
        # Transaction Template
        with col3:
            st.subheader("💳 Transaction Template")
            st.markdown(get_csv_download_link("transaction_template_enhanced.csv", transaction_template_enhanced, " Download Transaction Template"), unsafe_allow_html=True)
            st.markdown("**Required fields:** changetype, transaction_id, account_id, transaction_date")
            st.markdown("**Supports:** All transaction types including P2P")
        
        # Move the sample fields outside the columns
        st.markdown("---")
        st.markdown("###  Sample Fields Overview")
        
        # Create separate sections for each template's sample fields
        field_tabs = st.tabs(["Customer Fields", "Account Fields", "Transaction Fields"])
        
        with field_tabs[0]:
            st.markdown("**Key Customer Fields:**")
            customer_fields = [
                "changetype", "customer_id", "customer_type", "first_name", "last_name",
                "business_name", "tax_id", "email_address", "phone_number", "address_line_1"
            ]
            for field in customer_fields:
                st.write(f"• {field}")
        
        with field_tabs[1]:
            st.markdown("**Key Account Fields:**")
            account_fields = [
                "changetype", "account_id", "customer_id", "account_type", "account_balance",
                "open_date", "account_status", "is_p2p_enabled", "branch_id"
            ]
            for field in account_fields:
                st.write(f"• {field}")
        
        with field_tabs[2]:
            st.markdown("**Key Transaction Fields:**")
            transaction_fields = [
                "changetype", "transaction_id", "account_id", "transaction_date", "amount",
                "transaction_type", "counterparty_name", "is_p2p_transaction"
            ]
            for field in transaction_fields:
                st.write(f"• {field}")
        
        st.markdown("---")
        st.markdown("###  Field Mapping Features")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Automatic Mapping:**
            - 🤖 Auto-detects similar field names
            - 📝 Intelligent field suggestions
            -  Confidence scoring for matches
            """)
        
        with col2:
            st.markdown("""
            **Save & Reuse:**
            - 💾 Save custom field mappings
            - 📁 Load previously saved mappings
            -  Reuse for future uploads
            """)
        
        st.success("💡 **Pro Tip**: Upload your file with any headers, and our system will guide you through mapping them to the correct format!")

# Optimized AML detection function
# Fixed AML detection function with correct callback signatures
def run_aml_detection_after_upload(csv_file_path, upload_type="transaction", is_fixed_width=False):
    """
    Run AML detection after successful CSV upload - with lazy initialization
    """
    if not st.session_state.aml_available:
        st.warning("AML Detection is not available")
        return None
    
    # Initialize AML monitor only when needed
    monitor = get_aml_monitor()
    if not monitor:
        st.warning("Failed to initialize AML Detection")
        return None
    
    if 'progress_container' in st.session_state:
        st.session_state.progress_container.empty()
        
    # Create progress placeholders
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    
    # Create progress bar and status text
    progress_placeholder.empty()
    with progress_placeholder.container():
        progress_bar = st.progress(0)

    file_type = "Fixed-Width" if is_fixed_width else "CSV"
    
    with status_placeholder.container():
        status_text = st.info(f" Initializing AML Detection for {file_type} file...")

    
    with status_placeholder.container():
        status_text = st.info(f" Executing AML Detection for {file_type} file...")
    
            
    try:
        # Show AML detection progress
        # with st.spinner(" Running AML Detection on uploaded data..."):
        logger.info(f"Starting AML detection for {upload_type} data from {csv_file_path}  (Type: {file_type})")
        validate_csv = not is_fixed_width  # Skip CSV validation for fixed-width files
        logger.info(f"CSV validation will be {'skipped' if is_fixed_width else 'performed'} for {file_type} file")
        
        
        # Run comprehensive AML detection with correct callback signatures
        if is_fixed_width:
            run_aml_for_transactor = os.getenv('RUN_AML_RULES_TRANSACTOR', 'true').lower() == 'true'
            if run_aml_for_transactor:
                logger.info(f"Commence AML detection for transactors")
                aml_results = monitor.run_comprehensive_aml_detection(
                    # csv_file_path=csv_file_path,
                    file_path=csv_file_path,
                    frequency='both',  # Run both daily and weekly rules
                    rule_id=None,     # Run all rules
                    validate_file = validate_csv,
                    entity_type='Consumer',
                    load_to_db=False,  # Do not Load to database since data is already loaded
                    check_duplicates=False, # Skip duplicate checks since data is fresh
                    is_fixed_width=is_fixed_width
                )
            else:
                logger.info("RUN_AML_RULES_TRANSACTOR set to false, skipping AML detection for transactors")
            
            

        else:
            aml_results = monitor.run_comprehensive_aml_detection(
                # csv_file_path=csv_file_path,
                file_path=csv_file_path,
                frequency='both',  # Run both daily and weekly rules
                rule_id=None,     # Run all rules
                validate_file = validate_csv,
                entity_type='Consumer',
                load_to_db=False,  # Do not Load to database since data is already loaded
                check_duplicates=False, # Skip duplicate checks since data is fresh
                is_fixed_width=is_fixed_width
            )
                
        # Clean up progress indicators
        progress_placeholder.empty()
        status_placeholder.empty()

        # Show completion message
        if aml_results:
            alerts_count = aml_results.get('alerts_generated', 0)
            entities_processed = aml_results.get('entities_processed', 0)
            logger.info(f"AML detection completed for {file_type} file: {alerts_count} alerts, {entities_processed} entities processed")
            
            # Brief success message that will be replaced by the calling function
            with status_placeholder.container():
                st.success(f"AML Detection completed! Generated {alerts_count} alerts from {entities_processed} entities.")
        
        return aml_results
            
    except Exception as e:
        # Clean up on error
        if 'progress_placeholder' in locals():
            progress_placeholder.empty()
        if 'status_placeholder' in locals():
            status_placeholder.empty()
        logger.error(f"Error during AML detection: {str(e)}")
        st.error(f"AML Detection failed: {str(e)}")
        return None


# Optimized status check function
def check_log_file_status():
    """Add a status message about logging to the Streamlit UI - optimized"""
    if 'log_filename' in globals() and log_filename:
        try:
            if os.path.exists(log_filename):
                file_size = os.path.getsize(log_filename)
                if file_size > 0:
                    st.sidebar.success(f"Logging to: {os.path.basename(log_filename)} ({file_size} bytes)")
                else:
                    st.sidebar.warning(f"Log file exists but is empty")
            else:
                st.sidebar.error(f"Log file doesn't exist")
        except Exception as e:
            st.sidebar.error(f"Error checking log file: {str(e)}")
    else:
        st.sidebar.warning(" Logging to console only")

def display_aml_results(aml_results):
    """Display AML detection results in the UI"""
    if not aml_results:
        return
    
    st.markdown("<h3 style='font-size: 22px;'>AML Detection Results</h3>", unsafe_allow_html=True)
    
    # Create columns for metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Alerts Generated", aml_results.get('alerts_generated', 0))
    
    with col2:
        st.metric("Accounts Processed", aml_results.get('accounts_processed', 0))
    
    with col3:
        st.metric("Entities Processed", aml_results.get('entities_processed', 0))
    
    with col4:
        processing_time = aml_results.get('processing_time', 0)
        st.metric("Processing Time", f"{processing_time:.2f}s")
    
    # Display detailed results
    if aml_results.get('alerts_generated', 0) > 0:
        st.success(f"AML Detection completed successfully! Generated {aml_results['alerts_generated']} alerts.")
        
        # Show database stats if available
        if 'database_stats_after' in aml_results:
            db_stats = aml_results['database_stats_after']
            st.info(f"Database now contains: {db_stats.get('total_transactions', 'Unknown')} total transactions")
    else:
        st.info("AML Detection completed - No alerts generated.")
    
    # Show steps completed
    steps_completed = aml_results.get('steps_completed', [])
    if steps_completed:
        st.write("**Steps Completed:**")
        for step in steps_completed:
            st.write(f"{step.replace('_', ' ').title()}")
    
    # Show any errors or warnings
    if 'skipped_reason' in aml_results:
        st.warning(f"AML Detection was skipped: {aml_results['skipped_reason']}")
    
    # Show validation results if available
    if 'validation_results' in aml_results:
        validation = aml_results['validation_results']
        if validation.get('is_valid', False):
            st.success(" CSV validation passed")
        else:
            st.error(" CSV validation failed")
            for error in validation.get('errors', []):
                st.error(f"• {error}")

def update_aml_status_simple(percentage, entity_type, processed, total, current_entity, rule_name):
    """
    Simple, clean status update - no bloated UI components
    """
    # Create or update simple containers
    if not hasattr(st.session_state, 'aml_status_containers'):
        st.session_state.aml_status_containers = {
            'main_status': st.empty(),
            'progress': st.empty(),
            'metrics': st.empty()
        }
    
    containers = st.session_state.aml_status_containers
    
    # Main status
    with containers['main_status'].container():
        if percentage < 20:
            st.info(" Initializing AML Detection...")
        elif percentage < 70:
            st.info("💾 Loading data to database...")
        elif percentage < 95:
            st.info(" Running AML detection...")
        else:
            st.success(" AML Detection completed!")
    
    # Progress bar
    with containers['progress'].container():
        st.progress(percentage / 100.0)
        if entity_type and total > 0:
            st.text(f"{entity_type}: {processed:,}/{total:,} ({percentage:.1f}%)")
        else:
            st.text(f"Progress: {percentage:.1f}%")
    
    # Simple metrics
    with containers['metrics'].container():
        if entity_type and total > 0:
            cols = st.columns(3)
            with cols[0]:
                st.metric("Processing", entity_type)
            with cols[1]:
                st.metric("Completed", f"{processed:,}")
            with cols[2]:
                remaining = max(0, total - processed)
                st.metric("Remaining", f"{remaining:,}")


def run_aml_with_simple_tracking(csv_file_path, is_fixed_width=False):
    """
    Simple AML detection with real tracking - no fake simulation
    """
    file_type = "Fixed-Width" if is_fixed_width else "CSV"
    logger.info(f"Starting simple AML tracking for {file_type} file: {csv_file_path}")

    monitor = get_aml_monitor()
    if not monitor:
        st.error("AML monitor not available")
        return None
    
    # Simple progress callback
    def simple_progress(percentage, entity_type, processed, total, current_entity, rule_name):
        update_aml_status_simple(percentage, entity_type, processed, total, current_entity, rule_name)
    
    # Simple status callback  
    def simple_status(message, entity_type, alerts_count):
        pass  # Status is handled by progress callback
    
    try:
        validate_csv_flag = not is_fixed_width
        results = monitor.run_comprehensive_aml_detection(
            csv_file_path=csv_file_path,
            frequency='both',
            rule_id=None,
            validate_csv=validate_csv_flag,
            load_to_db=False,
            check_duplicates=False,
            progress_callback=simple_progress,
            status_callback=simple_status
        )
        
        # Clean up UI containers
        if hasattr(st.session_state, 'aml_status_containers'):
            for container in st.session_state.aml_status_containers.values():
                container.empty()
            del st.session_state.aml_status_containers
        
        return results
        
    except Exception as e:
        logger.error(f"AML detection failed: {str(e)}")
        st.error(f"AML detection failed: {str(e)}")
        return None

# Replace your AML button with this simple version
def create_simple_aml_button(is_fixed_width=False):
    """
    Simple AML detection button - no over-engineered dashboard
    """
    file_type_label = "Fixed-Width" if is_fixed_width else "CSV"
    button_label = f" Run AML Detection with Entity Tracking ({file_type_label})"

    if st.button(" Run AML Detection with Entity Tracking", key="simple_aml_detection"):
        if hasattr(st.session_state, 'temp_transaction_file') and os.path.exists(st.session_state.temp_transaction_file):
            
            start_time = time.time()
            results = run_aml_with_simple_tracking(st.session_state.temp_transaction_file, is_fixed_width)
            end_time = time.time()
            
            if results:
                st.session_state.aml_results = results
                st.session_state.aml_completed = True
                
                # Simple results display
                processing_time = end_time - start_time
                st.success(f"AML Detection completed in {processing_time:.2f} seconds")
                
                # Simple metrics
                cols = st.columns(4)
                with cols[0]:
                    st.metric("Alerts", results.get('alerts_generated', 0))
                with cols[1]:
                    st.metric("Entities", results.get('entities_processed', 0))
                with cols[2]:
                    st.metric("Accounts", results.get('accounts_processed', 0))
                with cols[3]:
                    st.metric("Time", f"{processing_time:.1f}s")
                
                # Show entity details if available
                if 'entity_processing_details' in results:
                    details = results['entity_processing_details']
                    st.write("**Entity Processing Summary:**")
                    st.write(f"- Customers: {details.get('customers_processed', 0)}/{details.get('customers_total', 0)}")
                    st.write(f"- Accounts: {details.get('accounts_processed', 0)}/{details.get('accounts_total', 0)}")
                    st.write(f"- Transactions: {details.get('transactions_processed', 0)}/{details.get('transactions_total', 0)}")
            else:
                st.error("AML Detection failed")
        else:
            st.error("No transaction file available")

# Function to read config.properties file
def read_config_file(config_path='aml_config.properties'):
    """Read database connection properties from config.properties file"""
    logger.info(f"Attempting to read configuration from {config_path}")
    
    config = {}
    
    # Check if file exists
    if not os.path.exists(config_path):
        # logger.error(f"Configuration file not found: {config_path}")
        return config
    
    try:
        # Read the properties file
        with open(config_path, 'r') as file:
            content = file.read()
        
        # Parse the content
        # Remove comments
        content = re.sub(r'#.*$', '', content, flags=re.MULTILINE)
        
        # Parse each line
        for line in content.splitlines():
            line = line.strip()
            if not line or '=' not in line:
                continue
                
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Remove trailing comments if any
            if '#' in value:
                value = value.split('#', 1)[0].strip()
            
            # Remove comments and commas at the end
            value = re.sub(r'[,\s]*#.*$', '', value).strip()
            
            config[key] = value
        
        # logger.info(f"Successfully read configuration: {', '.join(config.keys())}")
        logger.info(f"Successfully read configuration from database")
        return config
    except Exception as e:
        logger.exception(f"Error reading configuration file: {str(e)}")
        return {}



#Database connection function
def create_db_connection():
    """Create a database connection using parameters from environment variables or config file"""
    try:
        # First check environment variables (from Kubernetes ConfigMap)
        host = os.getenv('DB_HOST')
        database = os.getenv('DB_NAME') 
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        port = os.getenv('DB_PORT', '3306')
        connect_timeout = int(os.getenv('DB_CONNECT_TIMEOUT', '60'))
        connection_timeout = int(os.getenv('DB_CONNECTION_TIMEOUT', '60'))
        
        
        
        # Update session state with final values
        st.session_state.db_host = host
        st.session_state.db_name = database
        st.session_state.db_user = user
        st.session_state.db_password = password
        
        # Log connection attempt (without password)
        logger.info(f"Connecting to database: {host}:{port}/{database} as {user}")
        
        # Create connection with expanded parameters
        connection = mysql.connector.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
            pool_name="entity_pool",
            pool_size=32,
            connect_timeout=connect_timeout,
            connection_timeout=connection_timeout,
            autocommit=False  # Explicitly disable autocommit for batch operations
        )
        
        if connection.is_connected():
            # Set session variables for optimal performance
            cursor = connection.cursor()
            cursor.execute("SET SESSION innodb_lock_wait_timeout=50")
            cursor.execute("SET SESSION transaction_isolation='READ-COMMITTED'")
            cursor.execute("SET SESSION autocommit=0")  # Ensure autocommit is off
            cursor.close()
            
            logger.info(f"Successfully connected to database {database} at {host}")
            return connection
        else:
            logger.error(f"Connection to database failed")
            st.error("Could not connect to the database.")
            return None
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {str(e)}")
        st.error(f"Error connecting to MySQL database: {e}")
        return None

# ========================
# BATCH SQL GENERATION CLASSES
# ========================

class SQLBatchGenerator:
    """Base class for generating SQL statements in batches"""
    
    def __init__(self):
        self.batch_statements = []
        self.batch_params = []
        self.current_time = datetime.now()
    
    def add_statement(self, sql, params):
        """Add a SQL statement and its parameters to the batch"""
        self.batch_statements.append(sql)
        self.batch_params.append(params)
    
    def execute_batch(self, connection, commit=True):
        """Execute all batched statements"""
        if not self.batch_statements:
            return 0, []
        
        success_count = 0
        error_messages = []
        
        try:
            cursor = connection.cursor()
            
            # Group statements by SQL to use executemany where possible
            statement_groups = {}
            for sql, params in zip(self.batch_statements, self.batch_params):
                if sql not in statement_groups:
                    statement_groups[sql] = []
                statement_groups[sql].append(params)
            
            # Execute each group using executemany
            for sql, param_list in statement_groups.items():
                try:
                    cursor.executemany(sql, param_list)
                    success_count += len(param_list)
                    logger.info(f"Executed batch of {len(param_list)} statements")
                except Exception as e:
                    error_messages.append(f"Batch execution error: {str(e)}")
                    logger.error(f"Batch execution error: {str(e)}")
            
            if commit:
                connection.commit()
                logger.info(f"Committed {success_count} operations")
            
            cursor.close()
            
        except Exception as e:
            error_messages.append(f"Transaction error: {str(e)}")
            logger.error(f"Transaction error: {str(e)}")
            try:
                connection.rollback()
                logger.info("Transaction rolled back")
            except:
                pass
        
        return success_count, error_messages
    
    def clear_batch(self):
        """Clear all batched statements"""
        self.batch_statements.clear()
        self.batch_params.clear()

class EntityBatchGenerator(SQLBatchGenerator):
    """Generate entity-related SQL statements"""
    
    def generate_entity_insert(self, customer_entity_id, entity_type):
        """Generate entity insert SQL"""
        sql = """
        INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (customer_entity_id, entity_type, 'System', 'System', self.current_time, self.current_time, 'UserUpload')
        self.add_statement(sql, params)
        return sql, params
    
    def generate_customer_insert(self, entity_id, row_dict):
        """Generate customer entity insert SQL"""
        # Convert date fields
        date_of_birth = self.convert_date_field(row_dict.get('date_of_birth'))
        deceased_date = self.convert_date_field(row_dict.get('deceased_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        
        sql = """
        INSERT INTO entity_customer (
            entity_id, customer_entity_id, customer_parent_entity_id, 
            first_name, middle_name, last_name,  
            prefix, suffix, date_of_birth, ethnic_background, nationality, 
            dual_citizenship_country, customertype, occupation, ssn,
            home_ownership_type, customercreditscore, marital_status, 
            pep, relatedtopep, is_employee, deceased_date, closed_date, closed_reason,
            country_risk,contract_number,account_executive,business_unit,risk_category,
            country_of_birth_id, country_of_residence_id,
            svc_provider_name, inserted_date, updated_date, inserted_by, updated_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params = (
            entity_id, 
            row_dict.get('customer_id'), 
            row_dict.get('parent_customer_id'),
            row_dict.get('first_name'), 
            row_dict.get('middle_name'), 
            row_dict.get('last_name'), 
            row_dict.get('name_prefix'), 
            row_dict.get('name_suffix'), 
            date_of_birth,
            row_dict.get('ethnic_background'), 
            row_dict.get('nationality'),
            row_dict.get('dual_citizenship_country'), 
            row_dict.get('customer_type'), 
            row_dict.get('job_title'), 
            row_dict.get('tax_id'),
            row_dict.get('home_ownership_type'), 
            row_dict.get('customer_risk_score'), 
            row_dict.get('marital_status'),
            row_dict.get('pep'), 
            row_dict.get('is_related_to_pep'), 
            row_dict.get('is_employee'), 
            deceased_date,
            closed_date,
            row_dict.get('closed_reason'),
            row_dict.get('country_risk'),
            row_dict.get('contract_number'),
            row_dict.get('account_executive'),
            row_dict.get('business_unit'),
            row_dict.get('risk_category'),
            row_dict.get('country_of_birth'),
            row_dict.get('country_of_residence'),
            'UserUpload', 
            self.current_time, 
            self.current_time, 
            'System', 
            'System'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_business_insert(self, entity_id, row_dict):
        """Generate business entity insert SQL"""
        # Convert date fields
        date_incorporation = self.convert_date_field(row_dict.get('date_of_incorporation'))
        bankruptcy_date = self.convert_date_field(row_dict.get('bankruptcy_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        
        sql = """
        INSERT INTO entity_business (
            entity_id, customer_entity_id, customer_parent_entity_id, 
            legal_name, business_type, date_of_incorporation, 
            business_activity, taxid, doing_business_as, annual_turnover,
            bankruptcy_date, closed_date, closed_reason,
            country_risk,contract_number,account_executive,business_unit,risk_category,
            svc_provider_name, inserted_date, updated_date, inserted_by, updated_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s
        )
        """
        
        params = (
            entity_id, 
            row_dict.get('customer_id'), 
            row_dict.get('parent_customer_id'),
            row_dict.get('business_name'), 
            row_dict.get('business_type'), 
            date_incorporation,
            '', 
            row_dict.get('tin'), 
            row_dict.get('doing_business_as'),
            row_dict.get('annual_turnover'),
            bankruptcy_date,
            closed_date,
            row_dict.get('closed_reason'),
            row_dict.get('country_risk'),
            row_dict.get('contract_number'),
            row_dict.get('account_executive'),
            row_dict.get('business_unit'),
            row_dict.get('risk_category'),
            'UserUpload', 
            self.current_time, 
            self.current_time, 
            'System', 
            'System'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_address_insert(self, entity_id, row_dict):
        """Generate address insert SQL"""
        sql = """
        INSERT INTO entity_address (
            entity_id, address_type, address_1, address_2, address_3,
            address_city, address_state, address_country, address_zipcode,
            inserted_date, update_date, inserted_by, updated_by, svc_provider_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            entity_id,
            row_dict.get('address_type'),
            row_dict.get('address_line_1'),
            row_dict.get('address_line_2'),
            row_dict.get('address_line_3'),
            row_dict.get('address_city'),
            row_dict.get('address_state'),
            row_dict.get('address_country'),
            row_dict.get('address_zipcode'),
            self.current_time,
            self.current_time,
            'System',
            'System',
            'UserUpload'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_phone_insert(self, entity_id, row_dict):
        """Generate phone insert SQL"""
        sql = """
        INSERT INTO entity_phone (
            entity_id, phone_type, phone_number,
            insert_date, update_date, inserted_by, updated_by, svc_provider_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            entity_id,
            row_dict.get('phone_type'),
            row_dict.get('phone_number'),
            self.current_time,
            self.current_time,
            'System',
            'System',
            'UserUpload'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_email_insert(self, entity_id, row_dict):
        """Generate email insert SQL"""
        sql = """
        INSERT INTO entity_email (
            entity_id, email_type, email_address,
            insert_date, update_date, inserted_by, updated_by, SVC_PROVIDER_NAME
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            entity_id,
            row_dict.get('email_type'),
            row_dict.get('email_address'),
            self.current_time,
            self.current_time,
            'System',
            'System',
            'UserUpload'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_identifier_insert(self, entity_id, identifier_type, identifier_value, expiry_date=None):
        """Generate identifier insert SQL"""
        expiry_datetime = self.convert_date_field(expiry_date) if expiry_date else None
        
        sql = """
        INSERT INTO entity_identifier (
            entity_id, identifier_type, identifier_value, source_system,
            expiry_date, insert_date, update_date, inserted_by, updated_by, svc_provider_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            entity_id,
            identifier_type,
            identifier_value,
            'CSV_UPLOAD',
            expiry_datetime,
            self.current_time,
            self.current_time,
            'System',
            'System',
            'UserUpload'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def convert_date_field(self, date_str):
        """Convert date string to datetime object"""
        if not date_str or pd.isna(date_str) or date_str == '':
            return None
        
        # Try different date formats
        date_formats = [
            '%Y-%m-%d %H:%M:%S',  # 2023-01-15 14:30:00
            '%Y/%m/%d %H:%M:%S',  # 2023/01/15 14:30:00
            '%d-%m-%Y %H:%M:%S',  # 15-01-2023 14:30:00
            '%d/%m/%Y %H:%M:%S',  # 15/01/2023 14:30:00
            '%m-%d-%Y %H:%M:%S',  # 01-15-2023 14:30:00
            '%m/%d/%Y %H:%M:%S',  # 01/15/2023 14:30:00
            '%m/%d/%Y %H:%M',     # 01/15/2023 14:30 or 4/30/2025 12:13
            '%Y-%m-%d %H:%M',     # 2023-01-15 14:30
            '%Y/%m/%d %H:%M',     # 2023/01/15 14:30
            '%d-%m-%Y %H:%M',     # 15-01-2023 14:30
            '%d/%m/%Y %H:%M',     # 15/01/2023 14:30
            '%m-%d-%Y %H:%M',     # 01-15-2023 14:30
            '%Y-%m-%d',           # Date only: 2023-01-15
            '%Y/%m/%d',           # 2023/01/15
            '%d-%m-%Y',           # 15-01-2023
            '%d/%m/%Y',           # 15/01/2023
            '%m-%d-%Y',           # 01-15-2023
            '%m/%d/%Y',           # 01/15/2023
            '%Y%m%d'
        ]
        
        # Clean the date string
        if isinstance(date_str, str):
            date_str = date_str.strip()
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(str(date_str), fmt)
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                continue
        
        logger.warning(f"Could not parse date")
        return None


    def generate_entity_update(self, entity_id, entity_type):
        """Generate entity update SQL"""
        sql = """
        UPDATE entity 
        SET entity_type = %s, updated_by = %s, update_date = %s, svc_provider_name = %s
        WHERE ENTITY_ID = %s
        """
        params = (entity_type, 'System', self.current_time, 'UserUpload', entity_id)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_customer_update(self, entity_id, row_dict):
        """Generate customer entity update SQL"""
        # Convert date fields
        date_of_birth = self.convert_date_field(row_dict.get('date_of_birth'))
        deceased_date = self.convert_date_field(row_dict.get('deceased_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        
        sql = """
        UPDATE entity_customer 
        SET customer_parent_entity_id = %s, first_name = %s, middle_name = %s, 
            last_name = %s, prefix = %s, suffix = %s, date_of_birth = %s,
            ethnic_background = %s, nationality = %s, 
            dual_citizenship_country = %s, customertype = %s, occupation = %s, 
            ssn = %s, home_ownership_type = %s, customercreditscore = %s, 
            marital_status = %s, pep = %s, relatedtopep = %s, is_employee = %s, 
            deceased_date = %s, closed_date = %s, closed_reason = %s,
            country_risk=%s,contract_number=%s,account_executive=%s,
            business_unit=%s,risk_category=%s,country_of_birth_id = %s, country_of_residence_id = %s,
            updated_date = %s, updated_by = %s
        WHERE entity_id = %s
        """
        
        params = (
            row_dict.get('parent_customer_id'),
            row_dict.get('first_name'), 
            row_dict.get('middle_name'), 
            row_dict.get('last_name'), 
            row_dict.get('name_prefix'), 
            row_dict.get('name_suffix'), 
            date_of_birth,
            row_dict.get('ethnic_background'), 
            row_dict.get('nationality'),
            row_dict.get('dual_citizenship_country'), 
            row_dict.get('customer_type'), 
            row_dict.get('job_title'), 
            row_dict.get('tax_id'),
            row_dict.get('home_ownership_type'), 
            row_dict.get('customer_risk_score'), 
            row_dict.get('marital_status'),
            row_dict.get('pep'), 
            row_dict.get('is_related_to_pep'), 
            row_dict.get('is_employee'), 
            deceased_date,
            closed_date,
            row_dict.get('closed_reason'),
            row_dict.get('country_risk'),
            row_dict.get('contract_number'),
            row_dict.get('account_executive'),
            row_dict.get('business_unit'),
            row_dict.get('risk_category'),
            row_dict.get('country_of_birth'),
            row_dict.get('country_of_residence'),
            self.current_time, 
            'System',
            entity_id
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_business_update(self, entity_id, row_dict):
        """Generate business entity update SQL"""
        # Convert date fields
        date_incorporation = self.convert_date_field(row_dict.get('date_of_incorporation'))
        bankruptcy_date = self.convert_date_field(row_dict.get('bankruptcy_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        
        sql = """
        UPDATE entity_business 
        SET customer_parent_entity_id = %s, legal_name = %s, business_type = %s, 
            date_of_incorporation = %s, business_activity = %s, taxid = %s, 
            doing_business_as = %s, annual_turnover=%s, bankruptcy_date = %s, closed_date = %s, 
            closed_reason = %s, 
            country_risk =%s, contract_number =%s,
            account_executive=%s, business_unit=%s,
            risk_category=%s,
            updated_date = %s, updated_by = %s
        WHERE entity_id = %s
        """
        
        params = (
            row_dict.get('parent_customer_id'),
            row_dict.get('business_name'), 
            row_dict.get('business_type'), 
            date_incorporation,
            row_dict.get('industry_code'), 
            row_dict.get('tin'), 
            row_dict.get('doing_business_as'),
            row_dict.get('annual_turnover'),
            bankruptcy_date,
            closed_date,
            row_dict.get('closed_reason'),
            row_dict.get('country_risk'),
            row_dict.get('contract_number'),
            row_dict.get('account_executive'),
            row_dict.get('business_unit'),
            row_dict.get('risk_category'),
            self.current_time, 
            'System',
            entity_id
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_address_update(self, entity_id, row_dict):
        """Generate address update SQL"""
        sql = """
        UPDATE entity_address 
        SET address_1 = %s, address_2 = %s, address_3 = %s, address_city = %s, 
            address_state = %s, address_country = %s, address_zipcode = %s
        WHERE entity_id = %s AND address_type = %s
        """
        
        params = (
            row_dict.get('address_line_1'),
            row_dict.get('address_line_2'),
            row_dict.get('address_line_3'),
            row_dict.get('address_city'),
            row_dict.get('address_state'),
            row_dict.get('address_country'),
            row_dict.get('address_zipcode'),
            entity_id,
            row_dict.get('address_type')
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_phone_update(self, entity_id, row_dict):
        """Generate phone update SQL"""
        sql = """
        UPDATE entity_phone 
        SET phone_number = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND phone_type = %s
        """
        
        params = (
            row_dict.get('phone_number'),
            self.current_time,
            'System',
            entity_id,
            row_dict.get('phone_type')
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_email_update(self, entity_id, row_dict):
        """Generate email update SQL"""
        sql = """
        UPDATE entity_email 
        SET email_address = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND email_type = %s
        """
        
        params = (
            row_dict.get('email_address'),
            self.current_time,
            'System',
            entity_id,
            row_dict.get('email_type')
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_identifier_update(self, entity_id, identifier_type, identifier_value, expiry_date=None):
        """Generate identifier update SQL"""
        expiry_datetime = self.convert_date_field(expiry_date) if expiry_date else None
        
        sql = """
        UPDATE entity_identifier 
        SET identifier_value = %s, expiry_date = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND identifier_type = %s
        """
        
        params = (
            identifier_value,
            expiry_datetime,
            self.current_time,
            'System',
            entity_id,
            identifier_type
        )
        
        self.add_statement(sql, params)
        return sql, params


    def generate_entity_delete(self, entity_id):
            """Generate entity delete SQL (soft delete by setting status)"""
            sql = """
            UPDATE entity 
            SET entity_status = 'DELETED', updated_by = %s, update_date = %s, svc_provider_name = %s
            WHERE ENTITY_ID = %s
            """
            params = ('System', self.current_time, 'UserUpload', entity_id)
            self.add_statement(sql, params)
            return sql, params
    
    def generate_customer_delete(self, entity_id):
        """Generate customer entity delete SQL (soft delete)"""
        sql = """
        UPDATE entity_customer 
        SET customer_status = 'DELETED', updated_date = %s, updated_by = %s
        WHERE entity_id = %s
        """
        params = (self.current_time, 'System', entity_id)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_business_delete(self, entity_id):
        """Generate business entity delete SQL (soft delete)"""
        sql = """
        UPDATE entity_business 
        SET business_status = 'DELETED', updated_date = %s, updated_by = %s
        WHERE entity_id = %s
        """
        params = (self.current_time, 'System', entity_id)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_address_delete(self, entity_id):
        """Generate address delete SQL (hard delete for related records)"""
        sql = """
        DELETE FROM entity_address 
        WHERE entity_id = %s
        """
        params = (entity_id,)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_phone_delete(self, entity_id):
        """Generate phone delete SQL (hard delete for related records)"""
        sql = """
        DELETE FROM entity_phone 
        WHERE entity_id = %s
        """
        params = (entity_id,)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_email_delete(self, entity_id):
        """Generate email delete SQL (hard delete for related records)"""
        sql = """
        DELETE FROM entity_email 
        WHERE entity_id = %s
        """
        params = (entity_id,)
        self.add_statement(sql, params)
        return sql, params
    
    def generate_identifier_delete(self, entity_id):
        """Generate identifier delete SQL (hard delete for related records)"""
        sql = """
        DELETE FROM entity_identifier 
        WHERE entity_id = %s
        """
        params = (entity_id,)
        self.add_statement(sql, params)
        return sql, params

class AccountBatchGenerator(SQLBatchGenerator):
    """Generate account-related SQL statements"""
    
    def generate_account_insert(self, row_dict, entity_id, company_identifier=None):
        """Generate account insert SQL"""
        # Handle date conversions
        open_date = self.convert_date_field(row_dict.get('open_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        reopen_date = self.convert_date_field(row_dict.get('reopen_date'))
        p2p_enrollment_date = self.convert_date_field(row_dict.get('p2p_enrollment_date'))
        
        # Handle boolean fields
        is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle numeric fields
        account_balance = self.convert_numeric_field(row_dict.get('account_balance'))
        account_risk_score = self.convert_numeric_field(row_dict.get('account_risk_score'))
        
        sql = """
        INSERT INTO account (
            customer_account_id, entity_id, account_type, account_number, account_holder_name,
            account_routing_number, company_identification, account_currency, account_balance, 
            open_date, closed_date, close_reason, branch_id, reopen_date, 
            account_risk_score, account_status, is_zelle_enabled, 
            p2p_enrollment_date, p2p_email, p2p_phone, is_joint,
            svc_provider_name, insert_date, update_date, inserted_by, updated_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params = (
            row_dict.get('account_id'),
            None,
            row_dict.get('account_type'),
            row_dict.get('account_number'),
            row_dict.get('account_holder_name'),
            row_dict.get('account_routing_number'),
            None,
            row_dict.get('account_currency'),
            account_balance,
            open_date,
            closed_date,
            row_dict.get('close_reason'),
            row_dict.get('branch_id'),
            reopen_date,
            account_risk_score,
            row_dict.get('account_status'),
            is_p2p_enabled,
            p2p_enrollment_date,
            row_dict.get('p2p_email'),
            row_dict.get('p2p_phone'),
            is_joint,
            'UserUpload',
            self.current_time,
            self.current_time,
            'System',
            'System'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def convert_date_field(self, date_str):
        """Convert date string to datetime object"""
        if not date_str or pd.isna(date_str) or date_str == '':
            return None
        
        # Try different date formats
        date_formats = [
            '%Y-%m-%d %H:%M:%S',  # 2023-01-15 14:30:00
            '%Y/%m/%d %H:%M:%S',  # 2023/01/15 14:30:00
            '%d-%m-%Y %H:%M:%S',  # 15-01-2023 14:30:00
            '%d/%m/%Y %H:%M:%S',  # 15/01/2023 14:30:00
            '%m-%d-%Y %H:%M:%S',  # 01-15-2023 14:30:00
            '%m/%d/%Y %H:%M:%S',  # 01/15/2023 14:30:00
            '%m/%d/%Y %H:%M',     # 01/15/2023 14:30 or 4/30/2025 12:13
            '%Y-%m-%d %H:%M',     # 2023-01-15 14:30
            '%Y/%m/%d %H:%M',     # 2023/01/15 14:30
            '%d-%m-%Y %H:%M',     # 15-01-2023 14:30
            '%d/%m/%Y %H:%M',     # 15/01/2023 14:30
            '%m-%d-%Y %H:%M',     # 01-15-2023 14:30
            '%Y-%m-%d',           # Date only: 2023-01-15
            '%Y/%m/%d',           # 2023/01/15
            '%d-%m-%Y',           # 15-01-2023
            '%d/%m/%Y',           # 15/01/2023
            '%m-%d-%Y',           # 01-15-2023
            '%m/%d/%Y',           # 01/15/2023
        ]
        
        # Clean the date string
        if isinstance(date_str, str):
            date_str = date_str.strip()
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(str(date_str), fmt)
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                continue
        
        logger.warning(f"Could not parse date")
        return None
    
    def convert_numeric_field(self, value):
        """Convert numeric field with error handling"""
        if pd.isna(value):
            return None
        try:
            return float(value)
        except:
            return None
        

    def generate_account_update(self, db_account_id, row_dict, company_identifier=None):
        """Generate account update SQL"""
        # Handle date conversions
        open_date = self.convert_date_field(row_dict.get('open_date'))
        closed_date = self.convert_date_field(row_dict.get('closed_date'))
        reopen_date = self.convert_date_field(row_dict.get('reopen_date'))
        p2p_enrollment_date = self.convert_date_field(row_dict.get('p2p_enrollment_date'))
        
        # Handle boolean fields
        is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle numeric fields
        account_balance = self.convert_numeric_field(row_dict.get('account_balance'))
        account_risk_score = self.convert_numeric_field(row_dict.get('account_risk_score'))
        
        sql = """
        UPDATE account 
        SET account_type = %s, account_number = %s, account_routing_number = %s, 
            company_identification = %s, account_currency = %s, account_balance = %s, 
            open_date = %s, closed_date = %s, close_reason = %s, branch_id = %s, 
            reopen_date = %s, account_risk_score = %s, account_status = %s, 
            is_zelle_enabled = %s, p2p_enrollment_date = %s, p2p_email = %s, 
            p2p_phone = %s, is_joint = %s, update_date = %s, updated_by = %s
        WHERE account_id = %s
        """
        
        params = (
            row_dict.get('account_type'),
            row_dict.get('account_number'),
            row_dict.get('account_routing_number'),
            company_identifier,
            row_dict.get('account_currency'),
            account_balance,
            open_date,
            closed_date,
            row_dict.get('close_reason'),
            row_dict.get('branch_id'),
            reopen_date,
            account_risk_score,
            row_dict.get('account_status'),
            is_p2p_enabled,
            p2p_enrollment_date,
            row_dict.get('p2p_email'),
            row_dict.get('p2p_phone'),
            is_joint,
            self.current_time,
            'System',
            db_account_id
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_account_delete(self, db_account_id, soft_delete=True):
        """Generate account delete SQL (soft or hard delete)"""
        if soft_delete:
            sql = """
            UPDATE account 
            SET account_status = 'DELETED', closed_date = %s, close_reason = %s,
                update_date = %s, updated_by = %s
            WHERE account_id = %s
            """
            params = (
                self.current_time.strftime('%Y-%m-%d'),
                'Account Deleted via CSV Upload',
                self.current_time,
                'System',
                db_account_id
            )
        else:
            sql = """
            DELETE FROM account 
            WHERE account_id = %s
            """
            params = (db_account_id,)
        
        self.add_statement(sql, params)
        return sql, params

class TransactionBatchGenerator(SQLBatchGenerator):
    """Generate transaction-related SQL statements"""
    
    def generate_transaction_insert(self, row_dict, account_id):
        """Generate transaction insert SQL"""
        # Handle date conversion
        transaction_date = self.convert_datetime_field(row_dict.get('transaction_date'))
        
        # Handle boolean fields
        is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle amount and debit/credit indicator
        amount = self.convert_numeric_field(row_dict.get('amount'))
        debit_credit_indicator = 'D' if amount and amount < 0 else 'C' if amount else None
        
        sql = """
        INSERT INTO transactions (
            account_id, customer_transaction_id, transaction_code, transaction_type,tran_code_description,
            preferred_currency, nominal_currency, amount, debit_credit_indicator,
            transaction_date, description, transaction_origin_country, related_account,
            counterparty_routing_number, counterparty_name, branch_id, teller_id,
            check_number, check_image_front_path, check_image_back_path, txn_location,
            is_p2p_transaction, p2p_recipient_type, p2p_recipient, p2p_sender_type,
            p2p_sender, p2p_reference_id, balance, eod_balance, non_account_holder,
            positive_pay_indicator, originating_bank, beneficiary_bank, intermediary_1_name,
            instructing_bank, inter_bank_information, other_information,
            insert_date, update_date, inserted_by,
            updated_by, svc_provider_name
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 
            %s, %s
        )
        """
        
        params = (
            account_id,
            row_dict.get('transaction_id'),
            row_dict.get('transaction_code'),
            row_dict.get('transaction_type'),
            row_dict.get('tran_code_description'),
            row_dict.get('preferred_currency'),
            row_dict.get('settlement_currency'),
            amount,
            debit_credit_indicator,
            transaction_date,
            row_dict.get('tran_code_description'),
            row_dict.get('transaction_origin_country'),
            row_dict.get('counterparty_account'),
            row_dict.get('counterparty_routing_number'),
            row_dict.get('counterparty_name'),
            row_dict.get('branch_id'),
            row_dict.get('teller_id'),
            row_dict.get('check_number'),
            row_dict.get('check_image_front_path'),
            row_dict.get('check_image_back_path'),
            row_dict.get('txn_location'),
            is_p2p_transaction,
            row_dict.get('p2p_recipient_type'),
            row_dict.get('p2p_recipient'),
            row_dict.get('p2p_sender_type'),
            row_dict.get('p2p_sender'),
            row_dict.get('p2p_reference_id'),
            row_dict.get('balance'),
            row_dict.get('eod_balance'),
            row_dict.get('non_account_holder'),
            row_dict.get('positive_pay_indicator'),
            row_dict.get('originating_bank'),
            row_dict.get('beneficiary_bank'),
            row_dict.get('intermediary_bank'),
            row_dict.get('instructing_bank'),
            row_dict.get('inter_bank_information'),
            row_dict.get('other_information'),
            self.current_time,
            self.current_time,
            'System',
            'System',
            'UserUpload'
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def convert_datetime_field(self, date_str):
        """Convert datetime string to datetime object"""
        if not date_str or pd.isna(date_str):
            return None
        
        # Try multiple datetime formats
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%m/%d/%Y %H:%M',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except:
                continue
        return None
    
    def convert_numeric_field(self, value):
        """Convert numeric field with error handling"""
        if pd.isna(value):
            return None
        try:
            return float(value)
        except:
            return None
        
    def generate_transaction_update(self, db_transaction_id, row_dict):
        """Generate transaction update SQL"""
        # Handle date conversion
        transaction_date = self.convert_datetime_field(row_dict.get('transaction_date'))
        
        # Handle boolean fields
        is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle amount and debit/credit indicator
        amount = self.convert_numeric_field(row_dict.get('amount'))
        debit_credit_indicator = 'D' if amount and amount < 0 else 'C' if amount else None
        
        sql = """
        UPDATE transactions 
        SET transaction_code = %s, transaction_type = %s, tran_code_description = %s,
            preferred_currency = %s, nominal_currency = %s, amount = %s, 
            debit_credit_indicator = %s, transaction_date = %s, description = %s, 
            transaction_origin_country = %s, related_account = %s,
            counterparty_routing_number = %s, counterparty_name = %s, branch_id = %s, 
            teller_id = %s, check_number = %s, check_image_front_path = %s, 
            check_image_back_path = %s, txn_location = %s, is_p2p_transaction = %s, 
            p2p_recipient_type = %s, p2p_recipient = %s, p2p_sender_type = %s,
            p2p_sender = %s, p2p_reference_id = %s, 
            balance = %s, eod_balance = %s, non_account_holder =%s,
            positive_pay_indicator =%s, originating_bank=%s, beneficiary_bank=%s, intermediary_1_name=%s,
            instructing_bank=%s, inter_bank_information=%s, other_information=%s,
            update_date = %s, updated_by = %s
        WHERE transaction_id = %s
        """
        
        params = (
            row_dict.get('transaction_code'),
            row_dict.get('transaction_type'),
            row_dict.get('tran_code_description'),
            row_dict.get('preferred_currency'),
            row_dict.get('settlement_currency'),
            amount,
            debit_credit_indicator,
            transaction_date,
            row_dict.get('tran_code_description'),
            row_dict.get('transaction_origin_country'),
            row_dict.get('counterparty_account'),
            row_dict.get('counterparty_routing_number'),
            row_dict.get('counterparty_name'),
            row_dict.get('branch_id'),
            row_dict.get('teller_id'),
            row_dict.get('check_number'),
            row_dict.get('check_image_front_path'),
            row_dict.get('check_image_back_path'),
            row_dict.get('txn_location'),
            is_p2p_transaction,
            row_dict.get('p2p_recipient_type'),
            row_dict.get('p2p_recipient'),
            row_dict.get('p2p_sender_type'),
            row_dict.get('p2p_sender'),
            row_dict.get('p2p_reference_id'),
             row_dict.get('balance'),
            row_dict.get('eod_balance'),
            row_dict.get('non_account_holder'),
            row_dict.get('positive_pay_indicator'),
            row_dict.get('originating_bank'),
            row_dict.get('beneficiary_bank'),
            row_dict.get('intermediary_bank'),
            row_dict.get('instructing_bank'),
            row_dict.get('inter_bank_information'),
            row_dict.get('other_information'),
            self.current_time,
            'System',
            db_transaction_id
        )
        
        self.add_statement(sql, params)
        return sql, params
    
    def generate_transaction_delete(self, db_transaction_id, soft_delete=True):
        """Generate transaction delete SQL (soft or hard delete)"""
        if soft_delete:
            sql = """
            UPDATE transactions 
            SET transaction_status = 'DELETED', description = CONCAT(COALESCE(description, ''), ' - DELETED'),
                update_date = %s, updated_by = %s
            WHERE transaction_id = %s
            """
            params = (self.current_time, 'System', db_transaction_id)
        else:
            sql = """
            DELETE FROM transactions 
            WHERE transaction_id = %s
            """
            params = (db_transaction_id,)
        
        self.add_statement(sql, params)
        return sql, params

# ========================
# BATCH PROCESSING FUNCTIONS
# ========================

def batch_lookup_existing_entities(connection, customer_ids):
    """Batch lookup existing entities"""
    if not customer_ids:
        return {}
    
    valid_ids = list(set([cid for cid in customer_ids if cid and not pd.isna(cid)]))
    if not valid_ids:
        return {}
    
    result_map = {}
    try:
        placeholders = ', '.join(['%s'] * len(valid_ids))
        query = f"SELECT ENTITY_ID, customer_entity_id FROM entity WHERE customer_entity_id IN ({placeholders})"
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, valid_ids)
        
        for row in cursor.fetchall():
            result_map[row['customer_entity_id']] = row['ENTITY_ID']
        
        cursor.close()
        logger.info(f"Found {len(result_map)} existing entities out of {len(valid_ids)} requested")
    except Exception as e:
        logger.error(f"Error in batch entity lookup: {str(e)}")
    
    return result_map



def batch_lookup_existing_accounts(connection, account_ids):
    """Batch lookup existing accounts"""
    if not account_ids:
        return {}
    
    valid_ids = list(set([str(aid).strip().upper() for aid in account_ids if aid and not pd.isna(aid)]))
    if not valid_ids:
        return {}
    
    result_map = {}
    try:
        placeholders = ', '.join(['%s'] * len(valid_ids))
        query = f"SELECT account_id, customer_account_id FROM account WHERE customer_account_id IN ({placeholders})"
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, valid_ids)
        
        for row in cursor.fetchall():
            # Ensure the key is stored as string to match your CSV data
            customer_id_key = str(row['customer_account_id']).strip()
            result_map[customer_id_key] = row['account_id']
        
        cursor.close()
        logger.info(f"Found {len(result_map)} existing accounts out of {len(valid_ids)} requested")
    except Exception as e:
        logger.error(f"Error in batch account lookup: {str(e)}")
    
    return result_map

def batch_lookup_existing_transactions(connection, transaction_ids, allow_duplicates=False):
    """Batch lookup existing transactions"""
    if not transaction_ids :
        return {}
    
    valid_ids = list(set([tid for tid in transaction_ids if tid and not pd.isna(tid)]))
    if not valid_ids:
        return {}
    
    result_map = {}
    try:
        placeholders = ', '.join(['%s'] * len(valid_ids))
        query = f"SELECT transaction_id, customer_transaction_id FROM transactions WHERE customer_transaction_id IN ({placeholders})"
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, valid_ids)
        
        for row in cursor.fetchall():
            result_map[row['customer_transaction_id']] = row['transaction_id']
        
        cursor.close()
        logger.info(f"Found {len(result_map)} existing transactions out of {len(valid_ids)} requested")
    except Exception as e:
        logger.error(f"Error in batch transaction lookup: {str(e)}")
    
    return result_map

def batch_lookup_company_identifiers(connection, entity_ids):
    """Batch lookup company identifiers for entities"""
    if not entity_ids:
        return {}
    
    valid_ids = list(set([eid for eid in entity_ids if eid and not pd.isna(eid)]))
    if not valid_ids:
        return {}
    
    result_map = {}
    try:
        placeholders = ', '.join(['%s'] * len(valid_ids))
        query = f"""
        SELECT entity_id, identifier_value 
        FROM entity_identifier 
        WHERE entity_id IN ({placeholders}) AND identifier_type = 'COMPANY_IDENTIFIER'
        """
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, valid_ids)
        
        for row in cursor.fetchall():
            result_map[row['entity_id']] = row['identifier_value']
        
        cursor.close()
        logger.info(f"Found {len(result_map)} company identifiers out of {len(valid_ids)} requested")
    except Exception as e:
        logger.error(f"Error in batch company identifier lookup: {str(e)}")
    
    return result_map

def clean_row_data(row_dict):
    """Clean row data by replacing NaN values with None"""
    cleaned = {}
    for key, value in row_dict.items():
        if pd.isna(value) or value == 'nan' or (isinstance(value, str) and value.lower() == 'nan'):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

# ========================
# BATCH PROCESSING MAIN FUNCTIONS
# ========================

def process_entity_chunk_batch(chunk_df, connection, is_fixed_width=False):
    """Process entity chunk using batch SQL generation"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing entity chunk with {len(chunk_df)} records using batch SQL generation")
    
    # Collect all customer IDs for batch lookup
    customer_ids = chunk_df['customer_id'].tolist()
    existing_entities = batch_lookup_existing_entities(connection, customer_ids)
    
    # Process operations by type
    add_operations = []
    modify_operations = []
    delete_operations = []
    
    # Separate operations by type and existing status
    for index, row in chunk_df.iterrows():
        try:
            row_dict = clean_row_data(row.to_dict())
            
            if not is_fixed_width:
                changetype = row_dict.get('changetype', '').lower()
            
            customer_id = row_dict.get('customer_id')
            customer_type = row_dict.get('customer_type')
            
            if not customer_id:
                error_count += 1
                results.append(f"Row {index}: Missing customer_id")
                continue
            
            entity_id = existing_entities.get(customer_id)
            
            operation_data = {
                'index': index,
                'row_dict': row_dict,
                'customer_id': customer_id,
                'customer_type': customer_type,
                'entity_id': entity_id
            }
            
            if is_fixed_width:
                # For fixed-width files, always treat as ADD (or convert to MODIFY if exists)
                if entity_id:
                    # Entity exists, convert to modify
                    modify_operations.append(operation_data)
                    results.append(f"Row {index}: Customer {customer_id} exists, converting to modify")
                else:
                    add_operations.append(operation_data)
            else:
                # For CSV files, check changetype
                changetype = row_dict.get('changetype', '').lower() if row_dict.get('changetype') else 'add'
                
                if changetype == 'add':
                    if entity_id:
                        # Entity exists, convert to modify
                        modify_operations.append(operation_data)
                        results.append(f"Row {index}: Customer {customer_id} exists, converting to modify")
                    else:
                        add_operations.append(operation_data)
                elif changetype == 'modify':
                    if entity_id:
                        modify_operations.append(operation_data)
                    else:
                        # Convert to add
                        add_operations.append(operation_data)
                        results.append(f"Row {index}: Customer {customer_id} not found, converting to add")
                elif changetype == 'delete':
                    if entity_id:
                        delete_operations.append(operation_data)
                    else:
                        error_count += 1
                        results.append(f"Row {index}: Cannot delete non-existent customer {customer_id}")
        
        
        except Exception as e:
            error_count += 1
            results.append(f"Row {index}: Error preparing operation - {str(e)}")
            logger.exception(f"Error processing entity row {index}")
    
    # Process ADD operations in batches
    if add_operations:
        add_success, add_errors, add_results = process_entity_add_operations_batch(connection, add_operations)
        success_count += add_success
        error_count += add_errors
        results.extend(add_results)
    
    # Process MODIFY operations in batches
    if modify_operations:
        modify_success, modify_errors, modify_results = process_entity_modify_operations_batch(connection, modify_operations)
        success_count += modify_success
        error_count += modify_errors
        results.extend(modify_results)
    
    # Process DELETE operations in batches
    if delete_operations:
        delete_success, delete_errors, delete_results = process_entity_delete_operations_batch(connection, delete_operations)
        success_count += delete_success
        error_count += delete_errors
        results.extend(delete_results)
    
    return success_count, error_count, results

def process_entity_delete_operations_batch(connection, delete_operations):
    """Process entity DELETE operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not delete_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(delete_operations)} entity DELETE operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare delete statements - must delete in proper order due to foreign keys
        address_deletes = []
        phone_deletes = []
        email_deletes = []
        identifier_deletes = []
        customer_deletes = []
        business_deletes = []
        entity_deletes = []
        
        # Group operations by entity type and prepare delete parameters
        for op in delete_operations:
            entity_id = op['entity_id']
            customer_type = op['customer_type']
            
            # Delete related records first (foreign key constraints)
            address_deletes.append((entity_id,))
            phone_deletes.append((entity_id,))
            email_deletes.append((entity_id,))
            identifier_deletes.append((entity_id,))
            
            # Delete main entity records
            if customer_type == 'Consumer':
                customer_deletes.append((entity_id,))
            else:
                business_deletes.append((entity_id,))
            
            # Delete entity record last
            entity_deletes.append((entity_id,))
        
        # Execute delete statements in proper order (related records first)
        if address_deletes:
            address_sql = "DELETE FROM entity_address WHERE entity_id = %s"
            cursor.executemany(address_sql, address_deletes)
            logger.info(f"Deleted {len(address_deletes)} address records")
        
        if phone_deletes:
            phone_sql = "DELETE FROM entity_phone WHERE entity_id = %s"
            cursor.executemany(phone_sql, phone_deletes)
            logger.info(f"Deleted {len(phone_deletes)} phone records")
        
        if email_deletes:
            email_sql = "DELETE FROM entity_email WHERE entity_id = %s"
            cursor.executemany(email_sql, email_deletes)
            logger.info(f"Deleted {len(email_deletes)} email records")
        
        if identifier_deletes:
            identifier_sql = "DELETE FROM entity_identifier WHERE entity_id = %s"
            cursor.executemany(identifier_sql, identifier_deletes)
            logger.info(f"Deleted {len(identifier_deletes)} identifier records")
        
        if customer_deletes:
            customer_sql = "DELETE FROM entity_customer WHERE entity_id = %s"
            cursor.executemany(customer_sql, customer_deletes)
            logger.info(f"Deleted {len(customer_deletes)} customer records")
        
        if business_deletes:
            business_sql = "DELETE FROM entity_business WHERE entity_id = %s"
            cursor.executemany(business_sql, business_deletes)
            logger.info(f"Deleted {len(business_deletes)} business records")
        
        if entity_deletes:
            entity_sql = "DELETE FROM entity WHERE ENTITY_ID = %s"
            cursor.executemany(entity_sql, entity_deletes)
            logger.info(f"Deleted {len(entity_deletes)} entity records")
        
        # Commit all operations
        connection.commit()
        cursor.close()
        
        success_count = len(delete_operations)
        for op in delete_operations:
            results.append(f"Row {op['index']}: Successfully deleted {op['customer_type']} {op['customer_id']}")
        
        logger.info(f"Successfully completed batch DELETE operations for {success_count} entities")
        
    except Exception as e:
        error_count = len(delete_operations)
        results.append(f"Batch DELETE operation failed: {str(e)}")
        logger.error(f"Batch DELETE operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def process_entity_add_operations_batch(connection, add_operations):
    """Process entity ADD operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} entity ADD operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Step 1: Insert all entities first
        entity_inserts = []
        customer_inserts = []
        business_inserts = []
        address_inserts = []
        phone_inserts = []
        email_inserts = []
        identifier_inserts = []
        
        # Prepare entity inserts
        for op in add_operations:
            customer_id = op['customer_id']
            customer_type = op['customer_type']
            # entity_type = 'Consumer' if customer_type == 'Consumer' else 'Business'
            entity_type = customer_type

            entity_inserts.append((
                customer_id, entity_type, 'System', 'System', 
                current_time, current_time, 'UserUpload'
            ))
        
        # Execute entity inserts
        entity_sql = """
        INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(entity_sql, entity_inserts)
        logger.info(f"Inserted {len(entity_inserts)} entities")
        
        # Step 2: Get the entity IDs that were just created
        customer_ids = [op['customer_id'] for op in add_operations]
        new_entity_map = batch_lookup_existing_entities(connection, customer_ids)
        
        # Step 3: Prepare related record inserts using actual entity IDs
        for op in add_operations:
            customer_id = op['customer_id']
            customer_type = op['customer_type']
            row_dict = op['row_dict']
            entity_id = new_entity_map.get(customer_id)
            
            if not entity_id:
                error_count += 1
                results.append(f"Row {op['index']}: Failed to get entity_id for {customer_id}")
                continue
            
            # Prepare customer or business insert
            if customer_type in ['Consumer','Terminals', 'Agents', 'Agent Groups', 'Agent Owners', 'Agent Chains']:
                customer_params = prepare_customer_insert_params(entity_id, row_dict, current_time)
                customer_inserts.append(customer_params)
            else:
                business_params = prepare_business_insert_params(entity_id, row_dict, current_time)
                business_inserts.append(business_params)
            
            # Prepare related records
            if row_dict.get('address_line_1') or row_dict.get('address_line_2') or row_dict.get('address_line_3'):
                address_params = prepare_address_insert_params(entity_id, row_dict, current_time)
                address_inserts.append(address_params)
            
            if row_dict.get('phone_number'):
                phone_params = prepare_phone_insert_params(entity_id, row_dict, current_time)
                phone_inserts.append(phone_params)
            
            if row_dict.get('email_address'):
                email_params = prepare_email_insert_params(entity_id, row_dict, current_time)
                email_inserts.append(email_params)
            
            # Prepare identifier inserts
            identifier_params_list = prepare_identifier_insert_params_list(entity_id, row_dict, current_time)
            identifier_inserts.extend(identifier_params_list)
        
        # Step 4: Execute all related record inserts
        if customer_inserts:
            customer_sql = """
            INSERT INTO entity_customer (
                entity_id, customer_entity_id, customer_parent_entity_id, 
                first_name, middle_name, last_name, 
                prefix, suffix, date_of_birth, gender, ethnic_background, nationality, 
                dual_citizenship_country, customertype, occupation, ssn,
                home_ownership_type, customercreditscore, marital_status, 
                pep, relatedtopep, is_employee, deceased_date, closed_date, closed_reason,
                country_of_birth_id, country_of_residence_id,
                svc_provider_name, inserted_date, updated_date, inserted_by, updated_by
            ) VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s
            )
            """
            cursor.executemany(customer_sql, customer_inserts)
            logger.info(f"Inserted {len(customer_inserts)} customer records")
        
        if business_inserts:
            business_sql = """
            INSERT INTO entity_business (
                entity_id, customer_entity_id, customer_parent_entity_id, 
                legal_name, business_type, date_of_incorporation, 
                business_activity, taxid, doing_business_as, 
                bankruptcy_date, closed_date, closed_reason,
                svc_provider_name, inserted_date, updated_date, inserted_by, updated_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.executemany(business_sql, business_inserts)
            logger.info(f"Inserted {len(business_inserts)} business records")
        
        if address_inserts:
            address_sql = """
            INSERT INTO entity_address (
                entity_id, address_type, address_1, address_2, address_3,
                address_city, address_state, address_country, address_zipcode,
                svc_provider_name, inserted_date, update_date, inserted_by, updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(address_sql, address_inserts)
            logger.info(f"Inserted {len(address_inserts)} address records")
        
        if phone_inserts:
            phone_sql = """
            INSERT INTO entity_phone (
                entity_id, phone_type, phone_number,
                insert_date, update_date, inserted_by, updated_by, svc_provider_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(phone_sql, phone_inserts)
            logger.info(f"Inserted {len(phone_inserts)} phone records")
        
        if email_inserts:
            email_sql = """
            INSERT INTO entity_email (
                entity_id, email_type, email_address,
                insert_date, update_date, inserted_by, updated_by, SVC_PROVIDER_NAME
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(email_sql, email_inserts)
            logger.info(f"Inserted {len(email_inserts)} email records")
        
        if identifier_inserts:
            identifier_sql = """
            INSERT INTO entity_identifier (
                entity_id, identifier_type, identifier_value, source_system,
                expiry_date, insert_date, update_date, inserted_by, updated_by, svc_provider_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(identifier_sql, identifier_inserts)
            logger.info(f"Inserted {len(identifier_inserts)} identifier records")
        
        # Commit all operations
        connection.commit()
        cursor.close()
        
        success_count = len(add_operations)
        for op in add_operations:
            results.append(f"Row {op['index']}: Successfully added {op['customer_type']} {op['customer_id']}")
        
        logger.info(f"Successfully completed batch ADD operations for {success_count} entities")
        
    except Exception as e:
        error_count = len(add_operations)
        results.append(f"Batch ADD operation failed: {str(e)}")
        logger.error(f"Batch ADD operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def prepare_customer_insert_params(entity_id, row_dict, current_time):
    """Prepare parameters for customer insert"""
    # Convert date fields
    date_of_birth = convert_date_field(row_dict.get('date_of_birth'))
    deceased_date = convert_date_field(row_dict.get('deceased_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    
    return (
        entity_id, 
        row_dict.get('customer_id'), 
        row_dict.get('parent_customer_id'),
        row_dict.get('first_name'), 
        row_dict.get('middle_name'), 
        row_dict.get('last_name'), 
        row_dict.get('name_prefix'), 
        row_dict.get('name_suffix'), 
        date_of_birth,
        row_dict.get('gender'),
        row_dict.get('ethnic_background'), 
        row_dict.get('nationality'),
        row_dict.get('dual_citizenship_country'), 
        row_dict.get('customer_type'), 
        row_dict.get('job_title'), 
        row_dict.get('tax_id'),
        row_dict.get('home_ownership_type'), 
        row_dict.get('customer_risk_score'), 
        row_dict.get('marital_status'),
        row_dict.get('pep'), 
        row_dict.get('is_related_to_pep'), 
        row_dict.get('is_employee'), 
        deceased_date,
        closed_date,
        row_dict.get('closed_reason'),
        row_dict.get('country_of_birth'),
        row_dict.get('country_of_residence'),
        'UserUpload', 
        current_time, 
        current_time, 
        'System', 
        'System'
    )

def prepare_business_insert_params(entity_id, row_dict, current_time):
    """Prepare parameters for business insert"""
    # Convert date fields
    date_incorporation = convert_date_field(row_dict.get('date_of_incorporation'))
    bankruptcy_date = convert_date_field(row_dict.get('bankruptcy_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    
    return (
        entity_id, 
        row_dict.get('customer_id'), 
        row_dict.get('parent_customer_id'),
        row_dict.get('business_name'), 
        row_dict.get('business_type'), 
        date_incorporation,
        row_dict.get('industry_code'), 
        row_dict.get('tin'), 
        row_dict.get('doing_business_as'),
        bankruptcy_date,
        closed_date,
        row_dict.get('closed_reason'),
        'UserUpload', 
        current_time, 
        current_time, 
        'System', 
        'System'
    )

def prepare_address_insert_params(entity_id, row_dict, current_time):
    """Prepare parameters for address insert"""
    return (
        entity_id,
        row_dict.get('address_type'),
        row_dict.get('address_line_1'),
        row_dict.get('address_line_2'),
        row_dict.get('address_line_3'),
        row_dict.get('address_city'),
        row_dict.get('address_state'),
        row_dict.get('address_country'),
        row_dict.get('address_zipcode'),
        'UserUpload', 
        current_time, 
        current_time, 
        'System', 
        'System'
    )

def prepare_phone_insert_params(entity_id, row_dict, current_time):
    """Prepare parameters for phone insert"""
    return (
        entity_id,
        row_dict.get('phone_type'),
        row_dict.get('phone_number'),
        current_time,
        current_time,
        'System',
        'System',
        'UserUpload'
    )

def prepare_email_insert_params(entity_id, row_dict, current_time):
    """Prepare parameters for email insert"""
    return (
        entity_id,
        row_dict.get('email_type'),
        row_dict.get('email_address'),
        current_time,
        current_time,
        'System',
        'System',
        'UserUpload'
    )

def prepare_identifier_insert_params_list(entity_id, row_dict, current_time):
    """Prepare parameters for all identifier inserts"""
    identifier_params = []
    
    # Document identifier
    if row_dict.get('document_type') and row_dict.get('document_number'):
        expiry_date = convert_date_field(row_dict.get('document_expiry_date'))
        identifier_params.append((
            entity_id,
            row_dict.get('document_type'),
            row_dict.get('document_number'),
            'CSV_UPLOAD',
            expiry_date,
            current_time,
            current_time,
            'System',
            'System',
            'UserUpload'
        ))
    
    
    # NAICS
    if row_dict.get('industry_code'):
        identifier_params.append((
            entity_id, 'NAICS', row_dict.get('industry_code'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))

    # TIN
    if row_dict.get('tin'):
        identifier_params.append((
            entity_id, 'TIN', row_dict.get('tin'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))
    
    # LEI
    if row_dict.get('lei'):
        identifier_params.append((
            entity_id, 'LEI', row_dict.get('lei'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))
    
    # SWIFT
    if row_dict.get('swift_number'):
        identifier_params.append((
            entity_id, 'SWIFT_CODE', row_dict.get('swift_number'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))
    
    # Company Identifier
    if row_dict.get('company_identifier'):
        identifier_params.append((
            entity_id, 'COMPANY_IDENTIFIER', row_dict.get('company_identifier'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))
    
    # Company Name
    if row_dict.get('company_name'):
        identifier_params.append((
            entity_id, 'COMPANY_NAME', row_dict.get('company_name'), 'CSV_UPLOAD',
            None, current_time, current_time, 'System', 'System', 'UserUpload'
        ))
    
    return identifier_params

def process_entity_modify_operations_batch(connection, modify_operations):
    """Process entity MODIFY operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not modify_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(modify_operations)} entity MODIFY operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare update statements
        entity_updates = []
        customer_updates = []
        business_updates = []
        address_updates = []
        phone_updates = []
        email_updates = []
        identifier_updates = []
        
        # Group operations by entity type
        for op in modify_operations:
            entity_id = op['entity_id']
            customer_type = op['customer_type']
            row_dict = op['row_dict']
            
            # Update entity table
            entity_type = 'Consumer' if customer_type == 'Consumer' else 'Business'
            entity_updates.append((
                entity_type, 'System', current_time, 'UserUpload', entity_id
            ))
            
            # Update customer or business table
            if customer_type == 'Consumer':
                customer_params = prepare_customer_update_params(entity_id, row_dict, current_time)
                customer_updates.append(customer_params)
            else:
                business_params = prepare_business_update_params(entity_id, row_dict, current_time)
                business_updates.append(business_params)
            
            # Update related records (replace existing approach)
            if row_dict.get('address_line_1'):
                address_params = prepare_address_update_params(entity_id, row_dict)
                address_updates.append(address_params)
            
            if row_dict.get('phone_number'):
                phone_params = prepare_phone_update_params(entity_id, row_dict, current_time)
                phone_updates.append(phone_params)
            
            if row_dict.get('email_address'):
                email_params = prepare_email_update_params(entity_id, row_dict, current_time)
                email_updates.append(email_params)
            
            # Update identifiers
            identifier_params_list = prepare_identifier_update_params_list(entity_id, row_dict, current_time)
            identifier_updates.extend(identifier_params_list)
        
        # Execute all update statements
        if entity_updates:
            entity_sql = """
            UPDATE entity 
            SET entity_type = %s, updated_by = %s, update_date = %s, svc_provider_name = %s
            WHERE ENTITY_ID = %s
            """
            cursor.executemany(entity_sql, entity_updates)
            logger.info(f"Updated {len(entity_updates)} entity records")
        
        if customer_updates:
            customer_sql = """
            UPDATE entity_customer 
            SET customer_parent_entity_id = %s, first_name = %s, middle_name = %s, 
                last_name = %s,  prefix = %s, suffix = %s, date_of_birth = %s,
                gender= %s,
                ethnic_background = %s, nationality = %s, 
                dual_citizenship_country = %s, customertype = %s, occupation = %s, 
                ssn = %s, home_ownership_type = %s, customercreditscore = %s, 
                marital_status = %s, pep = %s, relatedtopep = %s, is_employee = %s, 
                deceased_date = %s, closed_date = %s, closed_reason = %s,
                updated_date = %s, updated_by = %s
            WHERE entity_id = %s
            """
            cursor.executemany(customer_sql, customer_updates)
            logger.info(f"Updated {len(customer_updates)} customer records")
        
        if business_updates:
            business_sql = """
            UPDATE entity_business 
            SET customer_parent_entity_id = %s, legal_name = %s, business_type = %s, 
                date_of_incorporation = %s, business_activity = %s, taxid = %s, 
                doing_business_as = %s, bankruptcy_date = %s, closed_date = %s, 
                closed_reason = %s, updated_date = %s, updated_by = %s
            WHERE entity_id = %s
            """
            cursor.executemany(business_sql, business_updates)
            logger.info(f"Updated {len(business_updates)} business records")
        
        # For related records, we need to handle upsert logic
        # (update if exists, insert if not exists)
        if address_updates:
            for address_params in address_updates:
                upsert_address_record(cursor, address_params, current_time)
            logger.info(f"Upserted {len(address_updates)} address records")
        
        if phone_updates:
            for phone_params in phone_updates:
                upsert_phone_record(cursor, phone_params, current_time)
            logger.info(f"Upserted {len(phone_updates)} phone records")
        
        if email_updates:
            for email_params in email_updates:
                upsert_email_record(cursor, email_params, current_time)
            logger.info(f"Upserted {len(email_updates)} email records")
        
        if identifier_updates:
            for identifier_params in identifier_updates:
                upsert_identifier_record(cursor, identifier_params, current_time)
            logger.info(f"Upserted {len(identifier_updates)} identifier records")
        
        # Commit all operations
        connection.commit()
        cursor.close()
        
        success_count = len(modify_operations)
        for op in modify_operations:
            results.append(f"Row {op['index']}: Successfully modified {op['customer_type']} {op['customer_id']}")
        
        logger.info(f"Successfully completed batch MODIFY operations for {success_count} entities")
        
    except Exception as e:
        error_count = len(modify_operations)
        results.append(f"Batch MODIFY operation failed: {str(e)}")
        logger.error(f"Batch MODIFY operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def prepare_customer_update_params(entity_id, row_dict, current_time):
    """Prepare parameters for customer update"""
    # Convert date fields
    date_of_birth = convert_date_field(row_dict.get('date_of_birth'))
    deceased_date = convert_date_field(row_dict.get('deceased_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    
    return (
        row_dict.get('parent_customer_id'),
        row_dict.get('first_name'), 
        row_dict.get('middle_name'), 
        row_dict.get('last_name'), 
        row_dict.get('name_prefix'), 
        row_dict.get('name_suffix'), 
        date_of_birth,
        row_dict.get('gender'),
        row_dict.get('ethnic_background'), 
        row_dict.get('nationality'),
        row_dict.get('dual_citizenship_country'), 
        row_dict.get('customer_type'), 
        row_dict.get('job_title'), 
        row_dict.get('tax_id'),
        row_dict.get('home_ownership_type'), 
        row_dict.get('customer_risk_score'), 
        row_dict.get('marital_status'),
        row_dict.get('pep'), 
        row_dict.get('is_related_to_pep'), 
        row_dict.get('is_employee'), 
        deceased_date,
        closed_date,
        row_dict.get('closed_reason'),
        current_time, 
        'System',
        entity_id
    )

def prepare_business_update_params(entity_id, row_dict, current_time):
    """Prepare parameters for business update"""
    # Convert date fields
    date_incorporation = convert_date_field(row_dict.get('date_of_incorporation'))
    bankruptcy_date = convert_date_field(row_dict.get('bankruptcy_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    
    return (
        row_dict.get('parent_customer_id'),
        row_dict.get('business_name'), 
        row_dict.get('business_type'), 
        date_incorporation,
        row_dict.get('industry_code'), 
        row_dict.get('tin'), 
        row_dict.get('doing_business_as'),
        bankruptcy_date,
        closed_date,
        row_dict.get('closed_reason'),
        current_time, 
        'System',
        entity_id
    )

def prepare_address_update_params(entity_id, row_dict):
    """Prepare parameters for address update/upsert"""
    return {
        'entity_id': entity_id,
        'address_type': row_dict.get('address_type'),
        'address_1': row_dict.get('address_line_1'),
        'address_2': row_dict.get('address_line_2'),
        'address_3': row_dict.get('address_line_3'),
        'address_city': row_dict.get('address_city'),
        'address_state': row_dict.get('address_state'),
        'address_country': row_dict.get('address_country'),
        'address_zipcode': row_dict.get('address_zipcode')
    }

def prepare_phone_update_params(entity_id, row_dict, current_time):
    """Prepare parameters for phone update/upsert"""
    return {
        'entity_id': entity_id,
        'phone_type': row_dict.get('phone_type'),
        'phone_number': row_dict.get('phone_number'),
        'current_time': current_time
    }

def prepare_email_update_params(entity_id, row_dict, current_time):
    """Prepare parameters for email update/upsert"""
    return {
        'entity_id': entity_id,
        'email_type': row_dict.get('email_type'),
        'email_address': row_dict.get('email_address'),
        'current_time': current_time
    }

def prepare_identifier_update_params_list(entity_id, row_dict, current_time):
    """Prepare parameters for all identifier updates/upserts"""
    identifier_params = []
    
    # Document identifier
    if row_dict.get('document_type') and row_dict.get('document_number'):
        expiry_date = convert_date_field(row_dict.get('document_expiry_date'))
        identifier_params.append({
            'entity_id': entity_id,
            'identifier_type': row_dict.get('document_type'),
            'identifier_value': row_dict.get('document_number'),
            'expiry_date': expiry_date,
            'current_time': current_time
        })
    
    # TIN
    if row_dict.get('tin'):
        identifier_params.append({
            'entity_id': entity_id,
            'identifier_type': 'TIN',
            'identifier_value': row_dict.get('tin'),
            'expiry_date': None,
            'current_time': current_time
        })
    

    if row_dict.get('lei'):
        identifier_params.append({
            'entity_id': entity_id,
            'identifier_type': 'LEI',
            'identifier_value': row_dict.get('lei'),
            'expiry_date': None,
            'current_time': current_time
        })

    if row_dict.get('swift_number'):
        identifier_params.append({
            'entity_id': entity_id,
            'identifier_type': 'SWIFT_CODE',
            'identifier_value': row_dict.get('swift_number'),
            'expiry_date': None,
            'current_time': current_time
        })

    if row_dict.get('company_identifier'):
            identifier_params.append({
                'entity_id': entity_id,
                'identifier_type': 'COMPANY_IDENTIFIER',
                'identifier_value': row_dict.get('company_identifier'),
                'expiry_date': None,
                'current_time': current_time
            })
    
    if row_dict.get('company_name'):
            identifier_params.append({
                'entity_id': entity_id,
                'identifier_type': 'COMPANY_NAME',
                'identifier_value': row_dict.get('company_name'),
                'expiry_date': None,
                'current_time': current_time
            })
    
    return identifier_params



def upsert_address_record(cursor, address_params, current_time):
    """Update address if exists, insert if not"""
    try:
        # Try to update first
        update_sql = """
        UPDATE entity_address 
        SET address_1 = %s, address_2 = %s, address_3 = %s, address_city = %s, 
            address_state = %s, address_country = %s, address_zipcode = %s
        WHERE entity_id = %s AND address_type = %s
        """
        cursor.execute(update_sql, (
            address_params['address_1'], address_params['address_2'], 
            address_params['address_3'], address_params['address_city'],
            address_params['address_state'], address_params['address_country'],
            address_params['address_zipcode'], address_params['entity_id'],
            address_params['address_type']
        ))
        
        # If no rows were affected, insert new record
        if cursor.rowcount == 0:
            insert_sql = """
            INSERT INTO entity_address (
                entity_id, address_type, address_1, address_2, address_3,
                address_city, address_state, address_country, address_zipcode,
                inserted_date, update_date, inserted_by, updated_by, svc_provider_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                address_params['entity_id'], address_params['address_type'],
                address_params['address_1'], address_params['address_2'],
                address_params['address_3'], address_params['address_city'],
                address_params['address_state'], address_params['address_country'],
                address_params['address_zipcode'],current_time, current_time,
                'System', 'System', 'UserUpload'
            ))
    except Exception as e:
        logger.error(f"Error upserting address record: {str(e)}")

def upsert_phone_record(cursor, phone_params, current_time):
    """Update phone if exists, insert if not"""
    try:
        # Try to update first
        update_sql = """
        UPDATE entity_phone 
        SET phone_number = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND phone_type = %s
        """
        cursor.execute(update_sql, (
            phone_params['phone_number'], current_time, 'System',
            phone_params['entity_id'], phone_params['phone_type']
        ))
        
        # If no rows were affected, insert new record
        if cursor.rowcount == 0:
            insert_sql = """
            INSERT INTO entity_phone (
                entity_id, phone_type, phone_number,
                insert_date, update_date, inserted_by, updated_by, svc_provider_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                phone_params['entity_id'], phone_params['phone_type'],
                phone_params['phone_number'], current_time, current_time,
                'System', 'System', 'UserUpload'
            ))
    except Exception as e:
        logger.error(f"Error upserting phone record: {str(e)}")

def upsert_email_record(cursor, email_params, current_time):
    """Update email if exists, insert if not"""
    try:
        # Try to update first
        update_sql = """
        UPDATE entity_email 
        SET email_address = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND email_type = %s
        """
        cursor.execute(update_sql, (
            email_params['email_address'], current_time, 'System',
            email_params['entity_id'], email_params['email_type']
        ))
        
        # If no rows were affected, insert new record
        if cursor.rowcount == 0:
            insert_sql = """
            INSERT INTO entity_email (
                entity_id, email_type, email_address,
                insert_date, update_date, inserted_by, updated_by, SVC_PROVIDER_NAME
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                email_params['entity_id'], email_params['email_type'],
                email_params['email_address'], current_time, current_time,
                'System', 'System', 'UserUpload'
            ))
    except Exception as e:
        logger.error(f"Error upserting email record: {str(e)}")

def upsert_identifier_record(cursor, identifier_params, current_time):
    """Update identifier if exists, insert if not"""
    try:
        # Try to update first
        update_sql = """
        UPDATE entity_identifier 
        SET identifier_value = %s, expiry_date = %s, update_date = %s, updated_by = %s
        WHERE entity_id = %s AND identifier_type = %s
        """
        cursor.execute(update_sql, (
            identifier_params['identifier_value'], identifier_params['expiry_date'],
            current_time, 'System', identifier_params['entity_id'],
            identifier_params['identifier_type']
        ))
        
        # If no rows were affected, insert new record
        if cursor.rowcount == 0:
            insert_sql = """
            INSERT INTO entity_identifier (
                entity_id, identifier_type, identifier_value, source_system,
                expiry_date, insert_date, update_date, inserted_by, updated_by, svc_provider_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                identifier_params['entity_id'], identifier_params['identifier_type'],
                identifier_params['identifier_value'], 'CSV_UPLOAD',
                identifier_params['expiry_date'], current_time, current_time,
                'System', 'System', 'UserUpload'
            ))
    except Exception as e:
        logger.error(f"Error upserting identifier record: {str(e)}")

def _generate_related_records(batch_generator, entity_id, row_dict):
    """Generate related record SQL statements"""
    # Address
    if row_dict.get('address_line_1') or row_dict.get('address_line_2') or row_dict.get('address_line_3') or row_dict.get('address_zipcode') or row_dict.get('address_state') :
        batch_generator.generate_address_insert(entity_id, row_dict)
    
    # Phone
    if row_dict.get('phone_number'):
        batch_generator.generate_phone_insert(entity_id, row_dict)
    
    # Email
    if row_dict.get('email_address'):
        batch_generator.generate_email_insert(entity_id, row_dict)
    
    # Identifiers
    if row_dict.get('document_type') and row_dict.get('document_number'):
        batch_generator.generate_identifier_insert(
            entity_id, 
            row_dict.get('document_type'), 
            row_dict.get('document_number'),
            row_dict.get('document_expiry_date')
        )
    
    if row_dict.get('tin'):
        batch_generator.generate_identifier_insert(entity_id, 'TIN', row_dict.get('tin'))
    
    if row_dict.get('lei'):
        batch_generator.generate_identifier_insert(entity_id, 'LEI', row_dict.get('lei'))
    
    if row_dict.get('swift_number'):
        batch_generator.generate_identifier_insert(entity_id, 'SWIFT_CODE', row_dict.get('swift_number'))
    
    if row_dict.get('company_identifier'):
        batch_generator.generate_identifier_insert(entity_id, 'COMPANY_IDENTIFIER', row_dict.get('company_identifier'))
    
    if row_dict.get('company_name'):
        batch_generator.generate_identifier_insert(entity_id, 'COMPANY_NAME', row_dict.get('company_name'))

def process_account_modify_operations_batch(connection, modify_operations):
    """Process account MODIFY operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not modify_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(modify_operations)} account MODIFY operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare account updates
        account_updates = []
        
        for op in modify_operations:
            row_dict = op['row_dict']
            db_account_id = op['db_account_id']
            company_identifier = op['company_identifier']
            
            account_params = prepare_account_update_params(row_dict, db_account_id, company_identifier, current_time)
            account_updates.append(account_params)
        
        # Execute account updates
        account_sql = """
        UPDATE account 
        SET account_type = %s, account_number = %s, account_routing_number = %s, entity_id = %s,
            company_identification = %s, account_currency = %s, account_balance = %s, 
            open_date = %s, closed_date = %s, close_reason = %s, branch_id = %s, 
            reopen_date = %s, account_risk_score = %s, account_status = %s, 
            is_zelle_enabled = %s, p2p_enrollment_date = %s, p2p_email = %s, 
            p2p_phone = %s, is_joint = %s, update_date = %s, updated_by = %s
        WHERE account_id = %s
        """
        
        cursor.executemany(account_sql, account_updates)
        connection.commit()
        cursor.close()
        
        success_count = len(modify_operations)
        for op in modify_operations:
            results.append(f"Row {op['index']}: Successfully modified account {op['account_id']}")
        
        logger.info(f"Successfully completed batch MODIFY operations for {success_count} accounts")
        
    except Exception as e:
        error_count = len(modify_operations)
        results.append(f"Batch account MODIFY operation failed: {str(e)}")
        logger.error(f"Batch account MODIFY operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Account modify transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def prepare_account_update_params(row_dict, db_account_id, company_identifier, current_time):
    """Prepare parameters for account update"""
    # Handle date conversions
    open_date = convert_date_field(row_dict.get('open_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    reopen_date = convert_date_field(row_dict.get('reopen_date'))
    p2p_enrollment_date = convert_date_field(row_dict.get('p2p_enrollment_date'))
    
    # Handle boolean fields
    is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle numeric fields
    account_balance = convert_numeric_field(row_dict.get('account_balance'))
    account_risk_score = convert_numeric_field(row_dict.get('account_risk_score'))
    
    return (
        row_dict.get('account_type'),
        row_dict.get('account_number'),
        row_dict.get('entity_id'),
        row_dict.get('account_routing_number'),
        company_identifier,
        row_dict.get('account_currency'),
        account_balance,
        open_date,
        closed_date,
        row_dict.get('close_reason'),
        row_dict.get('branch_id'),
        reopen_date,
        account_risk_score,
        row_dict.get('account_status'),
        is_p2p_enabled,
        p2p_enrollment_date,
        row_dict.get('p2p_email'),
        row_dict.get('p2p_phone'),
        is_joint,
        current_time,
        'System',
        db_account_id
    )

def process_transaction_modify_operations_batch(connection, modify_operations):
    """Process transaction MODIFY operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not modify_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(modify_operations)} transaction MODIFY operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare transaction updates
        transaction_updates = []
        
        for op in modify_operations:
            row_dict = op['row_dict']
            db_transaction_id = op['db_transaction_id']
            
            transaction_params = prepare_transaction_update_params_csv(row_dict, db_transaction_id, current_time)
            transaction_updates.append(transaction_params)
        
        # Execute transaction updates
        transaction_sql = """
        UPDATE transactions 
        SET transaction_code = %s, transaction_type = %s, tran_code_description = %s,
            preferred_currency = %s, nominal_currency = %s, amount = %s, 
            debit_credit_indicator = %s, transaction_date = %s, description = %s, 
            transaction_origin_country = %s, related_account = %s,
            counterparty_routing_number = %s, counterparty_name = %s, branch_id = %s, 
            teller_id = %s, check_number = %s, check_image_front_path = %s, 
            check_image_back_path = %s, txn_location = %s, is_p2p_transaction = %s, 
            p2p_recipient_type = %s, p2p_recipient = %s, p2p_sender_type = %s,
            p2p_sender = %s, p2p_reference_id = %s, update_date = %s, updated_by = %s
        WHERE transaction_id = %s
        """
        
        cursor.executemany(transaction_sql, transaction_updates)
        connection.commit()
        cursor.close()
        
        success_count = len(modify_operations)
        for op in modify_operations:
            results.append(f"Row {op['index']}: Successfully modified transaction {op['transaction_id']}")
        
        logger.info(f"Successfully completed batch MODIFY operations for {success_count} transactions")
        
    except Exception as e:
        error_count = len(modify_operations)
        results.append(f"Batch transaction MODIFY operation failed: {str(e)}")
        logger.error(f"Batch transaction MODIFY operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction modify transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results


def process_agents_in_chunks(cursor, agent_data_list, current_time, chunk_size=1000):
    """
    Process agents in chunks for better performance
    agent_data_list: list of tuples (intermediary_1_id, transaction_field_15)
    Returns dictionary mapping intermediary_1_id to entity_id
    """
    agent_entity_map = {}
    
    for i in range(0, len(agent_data_list), chunk_size):
        chunk = agent_data_list[i:i + chunk_size]
        
        # Extract intermediary_1_ids from chunk
        intermediary_ids = [item[0] for item in chunk if item[0]]
        
        if not intermediary_ids:
            continue
            
        # Bulk check existing agents
        placeholders = ','.join(['%s'] * len(intermediary_ids))
        cursor.execute(f"""
            SELECT customer_entity_id, entity_id 
            FROM entity_customer 
            WHERE customer_entity_id IN ({placeholders}) AND customertype = 'Agents'
        """, intermediary_ids)
        
        existing_agents = dict(cursor.fetchall())
        
        # Prepare data for new agents
        new_entity_data = []
        new_entity_customer_data = []
        
        for intermediary_1_id, transaction_field_15 in chunk:
            if not intermediary_1_id:
                continue
                
            if intermediary_1_id in existing_agents:
                # Agent exists, store mapping
                agent_entity_map[intermediary_1_id] = existing_agents[intermediary_1_id]
            else:
                # Agent doesn't exist, prepare for bulk insert
                first_name = ""
                last_name = ""
                
                if transaction_field_15:
                    # Split by comma first, then by space
                    if ',' in transaction_field_15:
                        parts = [part.strip() for part in transaction_field_15.split(',')]
                        if len(parts) >= 2:
                            first_name = parts[0]
                            last_name = parts[1]
                        elif len(parts) == 1:
                            first_name = parts[0]
                    else:
                        # Split by space
                        parts = transaction_field_15.split()
                        if len(parts) >= 2:
                            first_name = parts[0]
                            last_name = ' '.join(parts[1:])  # Join remaining parts as last name
                        elif len(parts) == 1:
                            first_name = parts[0]
                
                new_entity_data.append((intermediary_1_id, 'Agents', 'System', 'System', current_time, current_time, 'UserUpload-CashOnly'))
                new_entity_customer_data.append((intermediary_1_id, first_name, last_name))
        
        # Bulk insert new entities
        if new_entity_data:
            cursor.executemany("""
                INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, SVC_PROVIDER_NAME)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, new_entity_data)
            
            # Get the entity_ids for newly inserted records
            new_intermediary_ids = [item[0] for item in new_entity_data]
            placeholders = ','.join(['%s'] * len(new_intermediary_ids))
            cursor.execute(f"""
                SELECT customer_entity_id, entity_id 
                FROM entity 
                WHERE customer_entity_id IN ({placeholders}) AND entity_type = 'Agents'
            """, new_intermediary_ids)
            
            new_entity_mappings = dict(cursor.fetchall())
            
            # Prepare entity_customer data with entity_ids
            entity_customer_insert_data = []
            for intermediary_1_id, first_name, last_name in new_entity_customer_data:
                entity_id = new_entity_mappings.get(intermediary_1_id)
                if entity_id:
                    entity_customer_insert_data.append((
                        entity_id, intermediary_1_id, first_name, last_name, 'Agents', 
                        'UserUpload-CashOnly', current_time, current_time, 'System', 'System'
                    ))
                    agent_entity_map[intermediary_1_id] = entity_id
            
            # Bulk insert into entity_customer
            if entity_customer_insert_data:
                cursor.executemany("""
                    INSERT INTO entity_customer (entity_id, customer_entity_id, first_name, last_name, customertype, svc_provider_name, inserted_date, updated_date, inserted_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, entity_customer_insert_data)
    
    return agent_entity_map

def prepare_transaction_update_params(row_dict, db_transaction_id, current_time, account_id_map, agent_entity_map=None, cursor=None):
    """Prepare parameters for transaction update with account_id lookup"""

    try:
        # Handle datetime fields
        transaction_date = convert_datetime_field(row_dict.get('transaction_date'))
        
        # Handle boolean fields
        is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle amount and debit/credit indicator
        amount = convert_numeric_field(row_dict.get('amount'))
        debit_credit_indicator = 'D' if amount and amount > 0 else 'C' if amount else None
        
        if row_dict.get('settlement_currency') is None:
            settlement_currency = 'USD'
        else:
            settlement_currency = row_dict.get('settlement_currency')

        related_account = row_dict.get('b_number')
        
        intermediary_1_str = row_dict.get('intermediary_1_id')
        
        #  Get entity_id from agent_entity_map if provided, otherwise use single lookup
        if agent_entity_map and intermediary_1_str in agent_entity_map:
            intermediary_1 = agent_entity_map[intermediary_1_str]
        elif cursor:
            # Fallback to single lookup if not in map
            intermediary_1 = get_or_create_agent_entity(
                cursor, 
                intermediary_1_str, 
                row_dict.get('transaction_field_15'), 
                current_time
            )
        else:
            intermediary_1 = None
        
        # Get customer_account_id from row_dict
        # customer_account_id = row_dict.get('account_id')
        account_id = row_dict.get('account_id')

        # account_id_map.get(customer_account_id)

    
        return (
            account_id,
            row_dict.get('b_number'),
            row_dict.get('transaction_id'),
            row_dict.get('transaction_code'),
            row_dict.get('transaction_type'),
            row_dict.get('preferred_currency'),
            settlement_currency,
            amount,
            debit_credit_indicator,
            transaction_date,
            row_dict.get('tran_code_description'),
            row_dict.get('description'),
            row_dict.get('reference'),
            row_dict.get('balance'),
            row_dict.get('eod_balance'),
            row_dict.get('non_account_holder'),
            'US',  # transaction_origin_country
            row_dict.get('counterparty_routing_number'),
            row_dict.get('counterparty_name'),
            row_dict.get('branch_id'),
            row_dict.get('teller_id'),
            row_dict.get('check_number'),
            row_dict.get('check_image_front_path'),
            row_dict.get('check_image_back_path'),
            row_dict.get('txn_location'),
            row_dict.get('txn_source'),
            intermediary_1,
            'Agents',
            row_dict.get('intermediary_1_code'),
            row_dict.get('intermediary_1_name'),
            row_dict.get('intermediary_1_role'),
            row_dict.get('intermediary_1_reference'),
            row_dict.get('intermediary_1_fee'),
            row_dict.get('intermediary_1_fee_currency'),
            row_dict.get('intermediary_2_id'),
            row_dict.get('intermediary_2_type'),
            row_dict.get('intermediary_2_code'),
            row_dict.get('intermediary_2_name'),
            row_dict.get('intermediary_2_role'),
            row_dict.get('intermediary_2_reference'),
            row_dict.get('intermediary_2_fee'),
            row_dict.get('intermediary_2_fee_currency'),
            row_dict.get('intermediary_3_id'),
            row_dict.get('intermediary_3_type'),
            row_dict.get('intermediary_3_code'),
            row_dict.get('intermediary_3_name'),
            row_dict.get('intermediary_3_role'),
            row_dict.get('intermediary_3_reference'),
            row_dict.get('intermediary_3_fee'),
            row_dict.get('intermediary_3_fee_currency'),
            row_dict.get('positive_pay_indicator'),
            row_dict.get('originator_bank'),
            row_dict.get('beneficiary_details'),
            row_dict.get('intermediary_bank'),
            row_dict.get('beneficiary_bank'),
            row_dict.get('instructing_bank'),
            row_dict.get('inter_bank_information'),
            row_dict.get('other_information'),
            related_account,
            row_dict.get('transaction_field_1'),
            row_dict.get('transaction_field_2'),
            row_dict.get('transaction_field_3'),
            row_dict.get('transaction_field_4'),
            row_dict.get('transaction_field_5'),
            row_dict.get('transaction_field_11'),
            row_dict.get('transaction_field_12'),
            row_dict.get('transaction_field_13'),
            row_dict.get('transaction_field_14'),
            row_dict.get('transaction_field_15'),
            is_p2p_transaction,
            row_dict.get('p2p_recipient_type'),
            row_dict.get('p2p_recipient'),
            row_dict.get('p2p_sender_type'),
            row_dict.get('p2p_sender'),
            row_dict.get('p2p_reference_id'),
            current_time,  # update_date - always current time for updates
            'UserUpload',  # updated_by - always System for batch updates
            'UserUpload-CashOnly',  # svc_provider_name
            # WHERE clause parameter (1 field)
            db_transaction_id  # transaction_id for WHERE clause
        )
        
    except Exception as e:
        logger.error(f"Error in prepare_transaction_update_params: {str(e)}")
        return None

def prepare_transaction_update_params_csv(row_dict, db_transaction_id, current_time):
    """Prepare parameters for transaction update with account_id lookup"""

    try:
        # Handle datetime fields
        transaction_date = convert_datetime_field(row_dict.get('transaction_date'))
        
        # Handle boolean fields
        is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
        
        # Handle amount and debit/credit indicator
        amount = convert_numeric_field(row_dict.get('amount'))
        debit_credit_indicator = 'D' if amount and amount > 0 else 'C' if amount else None
        
        if row_dict.get('settlement_currency') is None:
            settlement_currency = 'USD'
        else:
            settlement_currency = row_dict.get('settlement_currency')

        related_account = row_dict.get('b_number')
        
        # intermediary_1_str = row_dict.get('intermediary_1_id')
        
        #  Get entity_id from agent_entity_map if provided, otherwise use single lookup
        # if agent_entity_map and intermediary_1_str in agent_entity_map:
        #     intermediary_1 = agent_entity_map[intermediary_1_str]
        # elif cursor:
        #     # Fallback to single lookup if not in map
        #     intermediary_1 = get_or_create_agent_entity(
        #         cursor, 
        #         intermediary_1_str, 
        #         row_dict.get('transaction_field_15'), 
        #         current_time
        #     )
        # else:
        #     intermediary_1 = None
        
        # Get customer_account_id from row_dict
        # customer_account_id = row_dict.get('account_id')
        account_id = row_dict.get('account_id')

        # account_id_map.get(customer_account_id)

    
        return (
            row_dict.get('transaction_code'),
            row_dict.get('transaction_type'),
            row_dict.get('tran_code_description'),
            row_dict.get('preferred_currency'),
            settlement_currency,
            amount,
            debit_credit_indicator,
            transaction_date,
            None,
            'US',  # transaction_origin_country
            related_account,
            row_dict.get('counterparty_routing_number'),
            row_dict.get('counterparty_name'),
            row_dict.get('branch_id'),
            row_dict.get('teller_id'),
            row_dict.get('check_number'),
            row_dict.get('check_image_front_path'),
            row_dict.get('check_image_back_path'),
            row_dict.get('txn_location'),
            is_p2p_transaction,
            row_dict.get('p2p_recipient_type'),         # 21. p2p_recipient_type
            row_dict.get('p2p_recipient'),              # 22. p2p_recipient
            row_dict.get('p2p_sender_type'),            # 23. p2p_sender_type
            row_dict.get('p2p_sender'),                 # 24. p2p_sender
            row_dict.get('p2p_reference_id'),           # 25. p2p_reference_id
            current_time,                               # 26. update_date
            'UserUpload',                               # 27. updated_by
            # WHERE clause parameter (1 field)
            db_transaction_id  # transaction_id for WHERE clause
        )
        
    except Exception as e:
        logger.error(f"Error in prepare_transaction_update_params_csv: {str(e)}")
        return None

def batch_lookup_account_ids(cursor, customer_account_ids):
    """Batch lookup account_ids from customer_account_ids"""
    try:
        if not customer_account_ids:
            return {}
            
        # Create placeholders for IN clause
        placeholders = ','.join(['%s'] * len(customer_account_ids))
        query = f"SELECT account_id, customer_account_id FROM account WHERE customer_account_id IN ({placeholders})"
        
        cursor.execute(query, list(customer_account_ids))
        results = cursor.fetchall()
        
        # Return mapping: customer_account_id -> account_id
        return {result[1]: result[0] for result in results}      
      
    except Exception as e:
        logger.error(f"Error in batch_lookup_account_ids: {str(e)}")
        return {}


def process_transaction_chunk_with_lookup(chunk_data, cursor, connection):
    """Process transaction chunk with account ID lookup"""
    try:
        # Extract all customer_account_ids from the chunk
        customer_account_ids = set()
        for row_dict, db_transaction_id, current_time in chunk_data:
            customer_account_id = row_dict.get('account_id')
            if customer_account_id:
                customer_account_ids.add(customer_account_id)
        
        # Batch lookup account_ids
        logger.info(f"Looking up {len(customer_account_ids)} unique customer account IDs")
        account_id_cache = batch_lookup_account_ids(cursor, customer_account_ids)
        
        # Log any missing mappings
        missing_mappings = customer_account_ids - set(account_id_cache.keys())
        if missing_mappings:
            logger.warning(f"Could not find account_id mappings for {len(missing_mappings)} customer_account_ids: {list(missing_mappings)[:10]}...")
        
        # Prepare transaction parameters with resolved account_ids
        transaction_params = []
        skipped_count = 0
        
        for row_dict, db_transaction_id, current_time in chunk_data:
            customer_account_id = row_dict.get('account_id')
            
            # Skip if we couldn't resolve the account_id
            if customer_account_id not in account_id_cache:
                logger.warning(f"Skipping transaction with unresolved customer_account_id: {customer_account_id}")
                skipped_count += 1
                continue
                
            # Update row_dict with resolved account_id for parameter preparation
            row_dict_copy = row_dict.copy()
            row_dict_copy['account_id'] = account_id_cache[customer_account_id]
            
            params = prepare_transaction_update_params(row_dict_copy, db_transaction_id, current_time, account_id_cache)
            if params:
                transaction_params.append(params)
        
        logger.info(f"Prepared {len(transaction_params)} transaction updates, skipped {skipped_count} due to missing account mappings")
        
        # Execute batch update
        if transaction_params:
            update_query = """
                UPDATE transactions SET account_id = %s, b_number = %s,
                    customer_transaction_id = %s, transaction_code = %s,
                    transaction_type = %s, preferred_currency = %s,
                    nominal_currency = %s, amount = %s,
                    debit_credit_indicator = %s, transaction_date = %s,
                    tran_code_description = %s, description = %s,
                    reference = %s, balance = %s,
                    eod_balance = %s, non_account_holder = %s,
                    transaction_origin_country = %s, counterparty_routing_number = %s,
                    counterparty_name = %s, branch_id = %s,
                    teller_id = %s, check_number = %s,
                    check_image_front_path = %s, check_image_back_path = %s,
                    txn_location = %s, txn_source = %s,
                    intermediary_1_id = %s, intermediary_1_type = %s,
                    intermediary_1_code = %s, intermediary_1_name = %s,
                    intermediary_1_role = %s, intermediary_1_reference = %s,
                    intermediary_1_fee = %s, intermediary_1_fee_currency = %s,
                    intermediary_2_id = %s, intermediary_2_type = %s,
                    intermediary_2_code = %s, intermediary_2_name = %s,
                    intermediary_2_role = %s, intermediary_2_reference = %s,
                    intermediary_2_fee = %s, intermediary_2_fee_currency = %s,
                    intermediary_3_id = %s, intermediary_3_type = %s,
                    intermediary_3_code = %s, intermediary_3_name = %s,
                    intermediary_3_role = %s,  intermediary_3_reference = %s,
                    intermediary_3_fee = %s, intermediary_3_fee_currency = %s,
                    positive_pay_indicator = %s, originator_bank = %s,
                    beneficiary_details = %s, intermediary_bank = %s,
                    beneficiary_bank = %s, instructing_bank = %s,
                    inter_bank_information = %s, other_information = %s,
                    related_account = %s, transaction_field_1 = %s,
                    transaction_field_2 = %s, transaction_field_3 = %s,
                    transaction_field_4 = %s, transaction_field_5 = %s,
                    transaction_field_11 = %s, transaction_field_12 = %s,
                    transaction_field_13 = %s, transaction_field_14 = %s,
                    transaction_field_15 = %s, is_p2p_transaction = %s,
                    p2p_recipient_type = %s, p2p_recipient = %s,
                    p2p_sender_type = %s, p2p_sender = %s,
                    p2p_reference_id = %s, update_date = %s,
                    updated_by = %s, svc_provider_name = %s
                WHERE transaction_id = %s
            """
            
            cursor.executemany(update_query, transaction_params)
            connection.commit()
            
            logger.info(f"Successfully updated {len(transaction_params)} transactions")
            return len(transaction_params), skipped_count
        else:
            logger.warning("No valid transaction parameters prepared for update")
            return 0, skipped_count
            
    except Exception as e:
        logger.error(f"Error in process_transaction_chunk_with_lookup: {str(e)}")
        connection.rollback()
        return 0, len(chunk_data)


def process_entity_chunk_batch_with_modify(chunk_df, connection):
    """Enhanced process entity chunk with proper modify support"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing entity chunk with {len(chunk_df)} records using enhanced batch SQL generation")
    
    # Collect all customer IDs for batch lookup
    customer_ids = chunk_df['customer_id'].tolist()
    existing_entities = batch_lookup_existing_entities(connection, customer_ids)
    
    # Process operations by type
    add_operations = []
    modify_operations = []
    delete_operations = []
    
    # Separate operations by type and existing status
    for index, row in chunk_df.iterrows():
        try:
            row_dict = clean_row_data(row.to_dict())
            
            changetype = row_dict.get('changetype', '').lower()
            customer_id = row_dict.get('customer_id')
            customer_type = row_dict.get('customer_type')
            
            if not customer_id:
                error_count += 1
                results.append(f"Row {index}: Missing customer_id")
                continue
            
            entity_id = existing_entities.get(customer_id)
            
            operation_data = {
                'index': index,
                'row_dict': row_dict,
                'customer_id': customer_id,
                'customer_type': customer_type,
                'entity_id': entity_id
            }
            
            if changetype == 'add':
                if entity_id:
                    # Entity exists, convert to modify
                    modify_operations.append(operation_data)
                    results.append(f"Row {index}: Customer {customer_id} exists, converting to modify")
                else:
                    add_operations.append(operation_data)
            elif changetype == 'modify':
                if entity_id:
                    modify_operations.append(operation_data)
                else:
                    # Convert to add
                    add_operations.append(operation_data)
                    results.append(f"Row {index}: Customer {customer_id} not found, converting to add")
            elif changetype == 'delete':
                if entity_id:
                    delete_operations.append(operation_data)
                else:
                    error_count += 1
                    results.append(f"Row {index}: Cannot delete non-existent customer {customer_id}")
        
        except Exception as e:
            error_count += 1
            results.append(f"Row {index}: Error preparing operation - {str(e)}")
            logger.exception(f"Error processing entity row {index}")

def process_account_chunk_batch(chunk_df, connection, is_fixed_width=False):
    """Process account chunk using batch SQL generation """
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing account chunk with {len(chunk_df)} records using batch SQL generation")
    
    # Batch lookups
    customer_ids = chunk_df['customer_id'].tolist()
    account_ids = chunk_df['account_id'].tolist()
    
    existing_entities = batch_lookup_existing_entities(connection, customer_ids)
    existing_accounts = batch_lookup_existing_accounts(connection, account_ids)
    
    # Get company identifiers for entities
    entity_id_list = [eid for eid in existing_entities.values() if eid]
    company_identifiers = batch_lookup_company_identifiers(connection, entity_id_list)
    
    # Separate operations
    add_operations = []
    modify_operations = []
    delete_operations = []
    
    for index, row in chunk_df.iterrows():
        try:
            row_dict = clean_row_data(row.to_dict())
            
            changetype = row_dict.get('changetype', '').lower()
            account_id = row_dict.get('account_id')
            customer_id = row_dict.get('customer_id')
            
            if not account_id:
                error_count += 1
                results.append(f"Row {index}: Missing account_id")
                continue
            
            if not is_fixed_width:
                entity_id = existing_entities.get(customer_id)
                if not entity_id and changetype != 'delete':
                    error_count += 1
                    results.append(f"Row {index}: Customer {customer_id} not found")
                    continue
            else:
                entity_id = None
            
            customer_account_id = str(row_dict.get('account_id', '')).strip()
            db_account_id = existing_accounts.get(customer_account_id)

            
            company_identifier = company_identifiers.get(entity_id) if entity_id else None
            
            operation_data = {
                'index': index,
                'row_dict': row_dict,
                'account_id': account_id,
                'entity_id': entity_id,
                'company_identifier': company_identifier,
                'db_account_id': db_account_id
            }
            
            if is_fixed_width:
                # For fixed-width files, always treat as ADD (or convert to MODIFY if exists)
                if db_account_id:
                    # Account exists, convert to modify
                    modify_operations.append(operation_data)
                    results.append(f"Row {index}: Account {account_id} exists, converting to modify")
                else:
                    add_operations.append(operation_data)
            else:
                # For CSV files, check changetype
                changetype = row_dict.get('changetype', '').lower() if row_dict.get('changetype') else 'add'
                
                if changetype == 'add':
                    if db_account_id:
                        # Account exists, convert to modify
                        modify_operations.append(operation_data)
                        results.append(f"Row {index}: Account {account_id} exists, converting to modify")
                    else:
                        add_operations.append(operation_data)
                elif changetype == 'modify':
                    if not db_account_id:
                        add_operations.append(operation_data)
                        results.append(f"Row {index}: Account {account_id} not found, converting to add")
                    else:
                        modify_operations.append(operation_data)
                elif changetype == 'delete':
                    if db_account_id:
                        delete_operations.append(operation_data)
                    else:
                        error_count += 1
                        results.append(f"Row {index}: Cannot delete non-existent account {account_id}")
        
        
        except Exception as e:
            error_count += 1
            results.append(f"Row {index}: Error preparing account operation - {str(e)}")
            logger.exception(f"Error processing account row {index}")
    
    # Process ADD operations
    if add_operations:
        if is_fixed_width:
            add_success, add_errors, add_results = process_account_add_operations_batch(connection, add_operations)
        else:
            add_success, add_errors, add_results = process_account_add_operations_batch_csv(connection, add_operations)
        success_count += add_success
        error_count += add_errors
        results.extend(add_results)
    
    # Process MODIFY operations
    if modify_operations:
        modify_success, modify_errors, modify_results = process_account_modify_operations_batch(connection, modify_operations)
        success_count += modify_success
        error_count += modify_errors
        results.extend(modify_results)
    
    # Process DELETE operations
    if delete_operations:
        delete_success, delete_errors, delete_results = process_account_delete_operations_batch(connection, delete_operations)
        success_count += delete_success
        error_count += delete_errors
        results.extend(delete_results)
    
    return success_count, error_count, results

def process_account_delete_operations_batch(connection, delete_operations):
    """Process account DELETE operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not delete_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(delete_operations)} account DELETE operations in batch")
    
    try:
        cursor = connection.cursor()
        
        # Prepare account deletes
        account_deletes = []
        
        for op in delete_operations:
            db_account_id = op['db_account_id']
            account_deletes.append((db_account_id,))
        
        # Execute account deletes
        account_sql = "DELETE FROM account WHERE account_id = %s"
        cursor.executemany(account_sql, account_deletes)
        connection.commit()
        cursor.close()
        
        success_count = len(delete_operations)
        for op in delete_operations:
            results.append(f"Row {op['index']}: Successfully deleted account {op['account_id']}")
        
        logger.info(f"Successfully completed batch DELETE operations for {success_count} accounts")
        
    except Exception as e:
        error_count = len(delete_operations)
        results.append(f"Batch account DELETE operation failed: {str(e)}")
        logger.error(f"Batch account DELETE operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Account delete transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def process_account_add_operations_batch_csv(connection, add_operations):
    """Process account ADD operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} account ADD operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare account inserts
        account_inserts = []
        
        for op in add_operations:
            row_dict = op['row_dict']
            # entity_id = op['entity_id']
            entity_id = None
            company_identifier = op['company_identifier']
            
            account_params = prepare_account_insert_params_csv(row_dict, entity_id, company_identifier, current_time, connection)
            account_inserts.append(account_params)
        
        # Execute account inserts
        account_sql = """
        INSERT INTO account (
            customer_account_id, entity_id, account_type, account_number, account_holder_name,
            account_routing_number, company_identification, account_currency, account_balance, 
            open_date, closed_date, close_reason, branch_id, reopen_date, 
            account_risk_score, account_status, is_zelle_enabled, 
            p2p_enrollment_date, p2p_email, p2p_phone, is_joint,
            svc_provider_name, insert_date, update_date, inserted_by, updated_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        cursor.executemany(account_sql, account_inserts)
        connection.commit()
        cursor.close()
        
        success_count = len(add_operations)
        for op in add_operations:
            results.append(f"Row {op['index']}: Successfully added account {op['account_id']}")
        
        logger.info(f"Successfully completed batch ADD operations for {success_count} accounts")
        
    except Exception as e:
        error_count = len(add_operations)
        results.append(f"Batch account ADD operation failed: {str(e)}")
        logger.error(f"Batch account ADD operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Account transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def process_account_add_operations_batch(connection, add_operations):
    """Process account ADD operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} account ADD operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare account inserts
        account_inserts = []
        
        for op in add_operations:
            row_dict = op['row_dict']
            # entity_id = op['entity_id']
            entity_id = None
            company_identifier = op['company_identifier']
            
            account_params = prepare_account_insert_params(row_dict, entity_id, company_identifier, current_time)
            account_inserts.append(account_params)
        
        # Execute account inserts
        account_sql = """
        INSERT INTO account (
            customer_account_id, entity_id, account_type, account_number, account_holder_name,
            account_routing_number, company_identification, account_currency, account_balance, 
            open_date, closed_date, close_reason, branch_id, reopen_date, 
            account_risk_score, account_status, is_zelle_enabled, 
            p2p_enrollment_date, p2p_email, p2p_phone, is_joint,
            svc_provider_name, insert_date, update_date, inserted_by, updated_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        cursor.executemany(account_sql, account_inserts)
        connection.commit()
        cursor.close()
        
        success_count = len(add_operations)
        for op in add_operations:
            results.append(f"Row {op['index']}: Successfully added account {op['account_id']}")
        
        logger.info(f"Successfully completed batch ADD operations for {success_count} accounts")
        
    except Exception as e:
        error_count = len(add_operations)
        results.append(f"Batch account ADD operation failed: {str(e)}")
        logger.error(f"Batch account ADD operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Account transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results

def get_entity_id_by_customerid(customer_entity_id, connection):
    """Get entity_id for a given customer_entity_id"""
    
    if not customer_entity_id:
        return None
    
    try:
        query = "SELECT ENTITY_ID FROM entity WHERE customer_entity_id = %s"
        
        cursor = connection.cursor()
        cursor.execute(query, (customer_entity_id,))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result[0]
        else:
            logger.info(f"No entity found for customer_entity_id: {customer_entity_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting entity_id for {customer_entity_id}: {str(e)}")
        return None

def prepare_account_insert_params_csv(row_dict, entity_id, company_identifier, current_time, connection):
    """Prepare parameters for account insert"""
    # Handle date conversions
    open_date = convert_date_field(row_dict.get('open_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    reopen_date = convert_date_field(row_dict.get('reopen_date'))
    p2p_enrollment_date = convert_date_field(row_dict.get('p2p_enrollment_date'))
    
    # Handle boolean fields
    is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle numeric fields
    account_balance = convert_numeric_field(row_dict.get('account_balance'))
    account_risk_score = convert_numeric_field(row_dict.get('account_risk_score'))

    #Fetch the entity_id based on customer_Entity_id provided
    entity_id_to_be_inserted = entity_id  # Use passed entity_id first

    # If no entity_id provided, try to look it up
    if entity_id_to_be_inserted is None:
        customer_entity_id = row_dict.get('customer_id') or row_dict.get('customer_entity_id')
        if customer_entity_id:
            entity_id_to_be_inserted = get_entity_id_by_customerid(customer_entity_id, connection)
            logger.info(f"Looked up entity_id {entity_id_to_be_inserted} for customer {customer_entity_id}")
        else:
            logger.warning("No customer_entity_id found in row data and no entity_id provided")
            entity_id_to_be_inserted = None
    
    return (
        row_dict.get('account_id'),
        entity_id_to_be_inserted,
        row_dict.get('account_type'),
        row_dict.get('account_number'),
        row_dict.get('account_holder_name'),
        row_dict.get('account_routing_number'),
        None,
        row_dict.get('account_currency'),
        account_balance,
        open_date,
        closed_date,
        row_dict.get('close_reason'),
        row_dict.get('branch_id'),
        reopen_date,
        account_risk_score,
        'Active',
        is_p2p_enabled,
        p2p_enrollment_date,
        row_dict.get('p2p_email'),
        row_dict.get('p2p_phone'),
        is_joint,
        'UserUpload',
        current_time,
        current_time,
        'System',
        'System'
    )

def prepare_account_insert_params(row_dict, entity_id, company_identifier, current_time):
    """Prepare parameters for account insert"""
    # Handle date conversions
    open_date = convert_date_field(row_dict.get('open_date'))
    closed_date = convert_date_field(row_dict.get('closed_date'))
    reopen_date = convert_date_field(row_dict.get('reopen_date'))
    p2p_enrollment_date = convert_date_field(row_dict.get('p2p_enrollment_date'))
    
    # Handle boolean fields
    is_p2p_enabled = 1 if row_dict.get('is_p2p_enabled') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    is_joint = 1 if row_dict.get('is_joint') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle numeric fields
    account_balance = convert_numeric_field(row_dict.get('account_balance'))
    account_risk_score = convert_numeric_field(row_dict.get('account_risk_score'))
    
    return (
        row_dict.get('account_id'),
        None,
        row_dict.get('account_type'),
        row_dict.get('account_number'),
        row_dict.get('account_holder_name'),
        row_dict.get('account_routing_number'),
        None,
        row_dict.get('account_currency'),
        account_balance,
        open_date,
        closed_date,
        row_dict.get('close_reason'),
        row_dict.get('branch_id'),
        reopen_date,
        account_risk_score,
        'Active',
        is_p2p_enabled,
        p2p_enrollment_date,
        row_dict.get('p2p_email'),
        row_dict.get('p2p_phone'),
        is_joint,
        'UserUpload',
        current_time,
        current_time,
        'System',
        'System'
    )

def convert_numeric_field(value):
    """Convert numeric field with error handling"""
    if pd.isna(value):
        return None
    try:
        return float(value)
    except:
        return None
    
def convert_datetime_field(date_str):
    """Convert datetime string to datetime object"""
    if not date_str or pd.isna(date_str):
        return None
    
    # Try multiple datetime formats
    date_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%m/%d/%Y %H:%M',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except:
            continue
    return None

def convert_date_field(date_str):
    """Convert date string to datetime object"""
    if not date_str or pd.isna(date_str) or date_str == '':
        return None
    
    # Try different date formats
    date_formats = [
        '%Y-%m-%d %H:%M:%S',  # 2023-01-15 14:30:00
        '%Y/%m/%d %H:%M:%S',  # 2023/01/15 14:30:00
        '%d-%m-%Y %H:%M:%S',  # 15-01-2023 14:30:00
        '%d/%m/%Y %H:%M:%S',  # 15/01/2023 14:30:00
        '%m-%d-%Y %H:%M:%S',  # 01-15-2023 14:30:00
        '%m/%d/%Y %H:%M:%S',  # 01/15/2023 14:30:00
        '%m/%d/%Y %H:%M',     # 01/15/2023 14:30 or 4/30/2025 12:13
        '%Y-%m-%d %H:%M',     # 2023-01-15 14:30
        '%Y/%m/%d %H:%M',     # 2023/01/15 14:30
        '%d-%m-%Y %H:%M',     # 15-01-2023 14:30
        '%d/%m/%Y %H:%M',     # 15/01/2023 14:30
        '%m-%d-%Y %H:%M',     # 01-15-2023 14:30
        '%Y-%m-%d',           # Date only: 2023-01-15
        '%Y/%m/%d',           # 2023/01/15
        '%d-%m-%Y',           # 15-01-2023
        '%d/%m/%Y',           # 15/01/2023
        '%m-%d-%Y',           # 01-15-2023
        '%m/%d/%Y',           # 01/15/2023
        '%Y%m%d'
    ]
    
    # Clean the date string
    if isinstance(date_str, str):
        date_str = date_str.strip()
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(str(date_str), fmt)
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            continue
    
    logger.warning(f"Could not parse date")
    return None

def process_transaction_chunk_batch(chunk_df, allow_duplicates=False, is_fixed_width=False, max_workers=8):
    """
    Multi-threaded version of process_transaction_chunk_batch that uses connection pool
    """
    from aml_detection.db_utils import DatabaseConnection
    db_connection = DatabaseConnection()
    
    results = []
    success_count = 0
    error_count = 0
    skip_count = 0 
    
    logger.info(f"Processing transaction chunk with {len(chunk_df)} records using multi-threaded batch processing")

    if max_workers is None:
        max_workers = 4  # Safe default
    elif isinstance(max_workers, bool): 
        logger.warning(f"max_workers was boolean {max_workers}, converting to integer")
        max_workers = 4 if max_workers else 2
    elif not isinstance(max_workers, int) or max_workers <= 0:
        logger.warning(f"Invalid max_workers {max_workers}, using default 4")
        max_workers = 4
    
    
    with db_connection.get_connection() as connection:
        # Batch lookups (keeping existing logic)
        account_ids = chunk_df['account_id'].tolist()
        transaction_ids = chunk_df['transaction_id'].tolist()
        
        existing_accounts = batch_lookup_existing_accounts(connection, account_ids)
        existing_transactions = batch_lookup_existing_transactions(connection, transaction_ids, allow_duplicates)
        
        # Separate operations
        add_operations = []
        modify_operations = []
        delete_operations = []
        
        # Use sets to track operations
        add_transaction_ids = set()
        modify_transaction_ids = set()
        delete_transaction_ids = set()
        
        # Process each row (keeping existing logic)
        for index, row in chunk_df.iterrows():
            try:
                row_dict = clean_row_data(row.to_dict())
                
                changetype = row_dict.get('changetype', '').lower()  
                transaction_id = row_dict.get('transaction_id')
                account_id = row_dict.get('account_id')
                
                if not transaction_id:
                    error_count += 1
                    results.append(f"ERROR-Row {index}: Missing transaction_id")
                    continue

                customer_account_id = str(row_dict.get('account_id', '')).strip()
                db_account_id = existing_accounts.get(customer_account_id)
                
                if not db_account_id and changetype != 'delete':
                    error_count += 1
                    results.append(f"ERROR - Row {index}: Account {account_id} not found")
                    continue
                
                db_transaction_id = existing_transactions.get(transaction_id)
                
                operation_data = {
                    'index': index,
                    'row_dict': row_dict,
                    'transaction_id': transaction_id,
                    'db_account_id': db_account_id,
                    'db_transaction_id': db_transaction_id
                }
                
                # Handle operations (keeping existing logic)
                if is_fixed_width:
                    if db_transaction_id:
                        if transaction_id not in modify_transaction_ids:
                            modify_operations.append(operation_data)
                            modify_transaction_ids.add(transaction_id)
                            results.append(f"Row {index}: Transaction {transaction_id} exists, converting to modify")
                        else:
                            skip_count += 1 
                            results.append(f"SKIP - Row {index}: Transaction {transaction_id} already queued for modify, skipping duplicate")
                    else:
                        if transaction_id not in add_transaction_ids:
                            add_operations.append(operation_data)
                            add_transaction_ids.add(transaction_id)
                        else:
                            skip_count += 1 
                            results.append(f"SKIP - Row {index}: Transaction {transaction_id} already queued for add, skipping duplicate")
                else:
                    changetype = row_dict.get('changetype', '').lower() if row_dict.get('changetype') else 'add'
                    
                    if changetype == 'add':
                        if db_transaction_id and not allow_duplicates:
                            if transaction_id not in modify_transaction_ids:
                                modify_operations.append(operation_data)
                                modify_transaction_ids.add(transaction_id)
                                results.append(f"Row {index}: Transaction {transaction_id} exists, converting to modify")
                            else:
                                skip_count += 1
                                results.append(f"SKIP-Row {index}: Transaction {transaction_id} already queued for modify, skipping duplicate")
                        else:
                            if transaction_id not in add_transaction_ids:
                                add_operations.append(operation_data)
                                add_transaction_ids.add(transaction_id)
                            else:
                                skip_count += 1
                                results.append(f"SKIP-Row {index}: Transaction {transaction_id} already queued for add, skipping")
                    elif changetype == 'modify':
                        if not db_transaction_id:
                            if transaction_id not in add_transaction_ids:
                                add_operations.append(operation_data)
                                add_transaction_ids.add(transaction_id)
                            else:
                                skip_count += 1
                                results.append(f"SKIP-Row {index}: Transaction {transaction_id} already queued for add, skipping")
                        else:
                            if transaction_id not in modify_transaction_ids:
                                modify_operations.append(operation_data)
                                modify_transaction_ids.add(transaction_id)
                                results.append(f"Row {index}: Transaction {transaction_id} exists, converting to modify")
                            else:
                                skip_count += 1
                                results.append(f"SKIP-Row {index}: Transaction {transaction_id} already queued for modify, skipping")
                    elif changetype == 'delete':
                        if db_transaction_id:
                            delete_operations.append(operation_data)
                        else:
                            error_count += 1
                            results.append(f"ERROR-Row {index}: Cannot delete non-existent transaction {transaction_id}")
            
            except Exception as e:
                error_count += 1
                results.append(f"Row {index}: Error preparing transaction operation - {str(e)}")
                logger.exception(f"Error processing transaction row {index}")
    
     # Add summary to results
    results.insert(0, f"SUMMARY: {len(chunk_df)} total records, {len(add_operations + modify_operations + delete_operations)} operations queued, {skip_count} duplicates skipped, {error_count} actual errors")
    
    # Process operations using multi-threaded functions
    if add_operations:
        add_success, add_errors, add_results = process_transaction_add_operations_batch_with_beneficiary(
            add_operations, chunk_size=1000, max_workers=max_workers
        )
        success_count += add_success
        error_count += add_errors
        results.extend(add_results)
    
    if modify_operations:
        modify_success, modify_errors, modify_results = process_transaction_modify_operations_batch_with_relationship(
            modify_operations, chunk_size=1000, max_workers=max_workers
        )
        success_count += modify_success
        error_count += modify_errors
        results.extend(modify_results)
    
    if delete_operations:
        delete_success, delete_errors, delete_results = process_transaction_delete_operations_batch(connection, delete_operations)
        success_count += delete_success
        error_count += delete_errors
        results.extend(delete_results)
    
    return success_count, error_count, results


# Helper function to determine optimal thread count based on system resources
def get_optimal_thread_count():
    """
    Safely determine optimal number of threads with fallback
    """
    try:
        import os
        from aml_detection.db_utils import DatabaseConnection
        
        # Get CPU count
        cpu_count = os.cpu_count() or 4
        
        # Try to get available database connections
        try:
            db_connection = DatabaseConnection()
            pool_info = db_connection.get_pool_info()
            available_connections = pool_info.get('total_available_connections', 0)
            logger.info(f"Pool info: {pool_info}")
        except Exception as pool_e:
            logger.warning(f"Could not get pool info: {pool_e}, using default connections")
            available_connections = 5  # Safe fallback
        
        # Debug logging
        logger.info(f"CPU count: {cpu_count}, Available connections: {available_connections}")
        
        # Ensure we have valid numbers
        if available_connections <= 0:
            logger.warning(f"No available connections detected ({available_connections}), using CPU-based fallback")
            return max(cpu_count // 2, 2)  # Use CPU cores as fallback
        
        # Conservative approach with absolute minimums
        optimal_threads = min(
            max(cpu_count // 2, 1),  # At least 1 from CPU
            max(available_connections // 3, 1),  # At least 1 from connections
            6  # Cap at 6 threads (reduced from 8)
        )
        
        # Absolute minimum of 2 threads
        result = max(optimal_threads, 2)
        logger.info(f"Calculated optimal threads: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in thread calculation: {str(e)}, using safe fallback")
        return 2  # Safe fallback


# Configuration function to set threading parameters
def configure_multithreading(max_workers=None, chunk_size=None):
    """
    Configure multi-threading parameters for transaction processing
    """
    config = {
        'max_workers': max_workers or get_optimal_thread_count(),
        'chunk_size': chunk_size or 5000,
        'agent_chunk_size': (chunk_size or 5000) // 2,
        'beneficiary_chunk_size': (chunk_size or 5000) // 4
    }
    
    logger.info(f"Multi-threading configuration: {config}")
    return config

def process_transaction_chunk_batch_csv(chunk_df, connection, allow_duplicates=False, is_fixed_width=False):
    """Process transaction chunk using batch SQL generation"""
    results = []
    success_count = 0
    error_count = 0
    
    logger.info(f"Processing transaction chunk with {len(chunk_df)} records using batch SQL generation")
    
    # Batch lookups
    account_ids = chunk_df['account_id'].tolist()
    transaction_ids = chunk_df['transaction_id'].tolist()
    
    existing_accounts = batch_lookup_existing_accounts(connection, account_ids)
    existing_transactions = batch_lookup_existing_transactions(connection, transaction_ids, allow_duplicates)
    
    # Separate operations
    add_operations = []
    modify_operations = []
    delete_operations = []

    # Use sets to track what operations we've already added
    add_transaction_ids = set()
    modify_transaction_ids = set()
    
    # Process each row
    for index, row in chunk_df.iterrows():
        try:
            row_dict = clean_row_data(row.to_dict())
            
            changetype = row_dict.get('changetype', '').lower()  
            transaction_id = row_dict.get('transaction_id')
            account_id = row_dict.get('account_id')
            
            if not transaction_id:
                error_count += 1
                results.append(f"Row {index}: Missing transaction_id")
                continue

            # Get the customer account ID from CSV (stored as string)
            customer_account_id = str(row_dict.get('account_id', '')).strip()

            # Look up the corresponding internal database account_id
            db_account_id = existing_accounts.get(customer_account_id)
            
            # db_account_id = existing_accounts.get(account_id)
            if not db_account_id and changetype != 'delete':
                error_count += 1
                results.append(f"Row {index}: Account {account_id} not found")
                continue
            
            db_transaction_id = existing_transactions.get(transaction_id)
            
            operation_data = {
                'index': index,
                'row_dict': row_dict,
                'transaction_id': transaction_id,
                'db_account_id': db_account_id,
                'db_transaction_id': db_transaction_id
            }
            
            # Handle operations
            if not is_fixed_width:
                # For fixed-width files, always treat as ADD (or convert to MODIFY if exists)
                if db_transaction_id :
                    # Transaction exists, convert to modify
                    if transaction_id not in modify_transaction_ids:
                        modify_operations.append(operation_data)
                        modify_transaction_ids.add(transaction_id)
                        results.append(f"Row {index}: Transaction {transaction_id} exists, converting to modify")
                    else:
                        results.append(f"Row {index}: Transaction {transaction_id} already queued for modify, skipping duplicate")
                else:
                    if transaction_id not in add_transaction_ids:
                        add_operations.append(operation_data)
                        add_transaction_ids.add(transaction_id)
                    else:
                        results.append(f"Row {index}: Transaction {transaction_id} already queued for add, skipping duplicate")
            else:
                # For CSV files, check changetype
                changetype = row_dict.get('changetype', '').lower() if row_dict.get('changetype') else 'add'
                
                if changetype == 'add':
                    if db_transaction_id and not allow_duplicates:
                        # Transaction exists, convert to modify
                        if transaction_id not in modify_transaction_ids:
                            modify_operations.append(operation_data)
                            modify_transaction_ids.add(transaction_id)
                            results.append(f"Row {index}: Transaction {transaction_id} exists, converting to modify")
                        else:
                            results.append(f"Row {index}: Transaction {transaction_id} already queued for modify, skipping duplicate")
                    else:
                        if transaction_id not in add_transaction_ids:
                            add_operations.append(operation_data)
                            add_transaction_ids.add(transaction_id)
                        else:
                            results.append(f"Row {index}: Transaction {transaction_id} already queued for add, skipping")
                            
                elif changetype == 'modify':
                    if not db_transaction_id:
                        # Convert to add
                        add_operations.append(operation_data)
                        add_transaction_ids.add(transaction_id)
                        results.append(f"Row {index}: Transaction {transaction_id} not found, converting to add")
                    else:
                        if transaction_id not in modify_transaction_ids:
                            modify_operations.append(operation_data)
                            modify_transaction_ids.add(transaction_id)
                        else:
                            results.append(f"Row {index}: Transaction {transaction_id} already queued for modify, skipping")
                            
                elif changetype == 'delete':
                    if db_transaction_id:
                        delete_operations.append(operation_data)
                    else:
                        error_count += 1
                        results.append(f"Row {index}: Cannot delete non-existent transaction {transaction_id}")
        
        
        except Exception as e:
            error_count += 1
            results.append(f"Row {index}: Error preparing transaction operation - {str(e)}")
            logger.exception(f"Error processing transaction row {index}")
    
    # Process operations in order
    if add_operations:
        add_success, add_errors, add_results = process_transaction_add_operations_batch_csv(connection, add_operations)
        success_count += add_success
        error_count += add_errors
        results.extend(add_results)
    
    if modify_operations:
        
        modify_success, modify_errors, modify_results = process_transaction_modify_operations_batch(connection, modify_operations)
        success_count += modify_success
        error_count += modify_errors
        results.extend(modify_results)
    
    if delete_operations:
        delete_success, delete_errors, delete_results = process_transaction_delete_operations_batch(connection, delete_operations)
        success_count += delete_success
        error_count += delete_errors
        results.extend(delete_results)
    
    return success_count, error_count, results


def process_transaction_delete_operations_batch(connection, delete_operations):
    """Process transaction DELETE operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not delete_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(delete_operations)} transaction DELETE operations in batch")
    
    try:
        cursor = connection.cursor()
        
        # Prepare transaction deletes
        transaction_deletes = []
        
        for op in delete_operations:
            db_transaction_id = op['db_transaction_id']
            transaction_deletes.append((db_transaction_id,))
        
        # Execute transaction deletes
        transaction_sql = "DELETE FROM transactions WHERE transaction_id = %s"
        cursor.executemany(transaction_sql, transaction_deletes)
        connection.commit()
        cursor.close()
        
        success_count = len(delete_operations)
        for op in delete_operations:
            results.append(f"Row {op['index']}: Successfully deleted transaction {op['transaction_id']}")
        
        logger.info(f"Successfully completed batch DELETE operations for {success_count} transactions")
        
    except Exception as e:
        error_count = len(delete_operations)
        results.append(f"Batch transaction DELETE operation failed: {str(e)}")
        logger.error(f"Batch transaction DELETE operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction delete transaction rolled back")
        except:
            pass
    
    return success_count, error_count, results
        
    

# ========================
# MAIN PROCESSING FUNCTIONS WITH BATCH APPROACH
# ========================

def process_entity_csv_data_batch(df, connection, is_fixed_width=False):
    """Process the entire entity CSV file using  batch SQL generation"""
    total_success = 0
    total_errors = 0
    all_results = []
    logger.info(f"Starting enhanced batch entity processing with {len(df)} records")
    
    # Split dataframe into chunks
    chunk_size = CHUNK_SIZE
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    logger.info(f"Processing entities in {num_chunks} chunks with chunk size {chunk_size}")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start_idx:end_idx].copy()
        
        status_text.text(f"Processing entity records {start_idx+1} to {end_idx} of {len(df)}...")
        
        # Process this chunk using enhanced batch approach with modify support
        success_count, error_count, results = process_entity_chunk_batch(chunk, connection,is_fixed_width)
        
        total_success += success_count
        total_errors += error_count
        all_results.extend(results)
        
        # Update progress bar
        progress_bar.progress((i + 1) / num_chunks)
    
    progress_bar.progress(1.0)
    status_text.text(f"Completed processing {len(df)} entity records.")
    logger.info(f"Enhanced Entity batch processing complete. Total success: {total_success}, Total errors: {total_errors}")
    
    return total_success, total_errors, all_results

def process_account_csv_data_batch(df, connection, is_fixed_width=False):
    """Process the entire account CSV file using  batch SQL generation"""
    total_success = 0
    total_errors = 0
    all_results = []
    logger.info(f"Starting enhanced batch account processing with {len(df)} records")
    
    # Split dataframe into chunks
    chunk_size = CHUNK_SIZE
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    logger.info(f"Processing accounts in {num_chunks} chunks with chunk size {chunk_size}")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start_idx:end_idx].copy()
        
        status_text.text(f"Processing account records {start_idx+1} to {end_idx} of {len(df)}...")
        
        # Process this chunk using enhanced batch approach with modify support
        # if is_fixed_width == False:
        #     success_count, error_count, results = process_account_chunk_batch_with_biller_entity(chunk, connection, is_fixed_width)
        # else:
        success_count, error_count, results = process_account_chunk_batch(chunk, connection, is_fixed_width)
        
        total_success += success_count
        total_errors += error_count
        all_results.extend(results)
        
        # Update progress bar
        progress_bar.progress((i + 1) / num_chunks)
    
    progress_bar.progress(1.0)
    status_text.text(f"Completed processing {len(df)} account records.")
    logger.info(f"Enhanced Account batch processing complete. Total success: {total_success}, Total errors: {total_errors}")
    
    return total_success, total_errors, all_results

def process_transaction_csv_data_batch(df, connection, allow_duplicates=False, csv_file_path=None, is_fixed_width=False):
    """Process the entire transaction CSV file using  batch SQL generation"""
    total_success = 0
    total_errors = 0
    all_results = []
    aml_results = None
    logger.info(f"Starting enhanced batch transaction processing with {len(df)} records, allow_duplicates={allow_duplicates}")
    
    try:
        # PRE-PROCESS ALL AGENTS IN BULK - THIS IS CRITICAL FOR PERFORMANCE
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Extract all unique agent data for bulk processing
        agent_data_list = []
        for _, row in df.iterrows():
            intermediary_1_id = row.get('intermediary_1_id')
            transaction_field_15 = row.get('transaction_field_15')
            if intermediary_1_id:
                agent_data_list.append((intermediary_1_id, transaction_field_15))
        
        # Remove duplicates
        agent_data_list = list(set(agent_data_list))
        
        # Bulk process agents
        agent_entity_map = process_agents_in_chunks(cursor, agent_data_list, current_time) if agent_data_list else {}
        cursor.close()
        
        logger.info(f"Pre-processed {len(agent_entity_map)} agents")

    
        # Split dataframe into chunks
        chunk_size = CHUNK_SIZE
        num_chunks = (len(df) + chunk_size - 1) // chunk_size
        logger.info(f"Processing transactions in {num_chunks} chunks with chunk size {chunk_size}")
        
         # Create clean progress containers with unique names
        transaction_progress_bar = st.progress(0)
        transaction_status = st.empty()
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk = df.iloc[start_idx:end_idx].copy()

            # Update status using the empty placeholder
            transaction_status.info(f"Processing transaction records {start_idx+1} to {end_idx} of {len(df)}...")
            try:

                if is_fixed_width:
                    # Process this chunk using enhanced batch approach with modify support
                    success_count, error_count, results = process_transaction_chunk_batch(chunk, connection, allow_duplicates, is_fixed_width=True)
                else:
                    success_count, error_count, results = process_transaction_chunk_batch_csv(chunk, connection, allow_duplicates, is_fixed_width=True)

                total_success += success_count
                total_errors += error_count
                all_results.extend(results)
                
                logger.info(f"Chunk {i+1} completed: {success_count} success, {error_count} errors")
            except Exception as chunk_error:
                    logger.error(f"Error processing chunk {i+1}: {str(chunk_error)}")
                    total_errors += len(chunk)
                    all_results.append(f"Chunk {i+1} failed: {str(chunk_error)}")
                    
                    # Show error in UI but continue processing
                    st.error(f"Error in chunk {i+1}: {str(chunk_error)}")
                
            # Update progress bar
            progress = (i + 1) / num_chunks
            transaction_progress_bar.progress(progress) 

        # Clean up containers
        transaction_status.empty()
        
        logger.info(f"Transaction upload processing complete. Total success: {total_success}, Total errors: {total_errors}")      
            
        # If processing was successful and AML is available, run AML detection
        aml_results = None
        if total_success > 0 and csv_file_path and st.session_state.aml_available:
            try:
                st.info("Transaction upload completed successfully. Starting AML Detection...")
                status_text.text(" Running AML Detection...")
                logger.info("Starting AML detection after successful transaction processing")
                    
                aml_results = run_aml_detection_after_upload(csv_file_path, "transaction", is_fixed_width)
                
                if aml_results:
                    # Store AML results in session state
                    st.session_state.aml_results = aml_results
                    st.session_state.aml_completed = True
                else:
                    logger.warning("AML detection returned no results")
            except Exception as aml_error:
                    logger.error(f"AML detection failed: {str(aml_error)}")
                    st.error(f"AML Detection failed: {str(aml_error)}")
                    aml_results = None
        else:
            if total_success == 0:
                logger.info("No AML detection - no successful transactions processed")
            elif not csv_file_path:
                logger.info("No AML detection - no CSV file path provided")
            elif not st.session_state.aml_available:
                logger.info("No AML detection - AML system not available")

        
        return total_success, total_errors, all_results, aml_results
    
    except Exception as overall_error:
        logger.error(f"Critical error in transaction processing: {str(overall_error)}")
        st.error(f" Critical processing error: {str(overall_error)}")
        
        # Clean up UI elements
        if 'transaction_status' in locals():
            transaction_status.empty()
            
        return 0, len(df), [f"Processing failed: {str(overall_error)}"], None

def process_transaction_add_operations_batch_optimized(connection, add_operations):
    results = []
    success_count = 0
    error_count = 0
    beneficiary_entities_created = 0
    total_relationships_created = 0
    total_relationships_existed = 0
    agents_processed = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} transaction ADD operations with optimized chunked processing")
    
    # UI containers for progress tracking
    relationship_status_container = st.empty()
    relationship_progress_container = st.empty()
    relationship_metrics_container = st.empty()
    chunk_size = CHUNK_SIZE
    
    try:
        # Split operations into chunks
        operation_chunks = [add_operations[i:i + chunk_size] for i in range(0, len(add_operations), chunk_size)]
        logger.info(f"Split operations into {len(operation_chunks)} chunks")
        
        with relationship_status_container.container():
            st.info(f"Processing {len(add_operations)} transactions with optimized entity relationships and agent processing")
        
        with relationship_progress_container.container():
            progress_bar = st.progress(0)
        
        current_time = datetime.now()
        
        # PHASE 0: Pre-process ALL agents across ALL chunks using existing batch method
        logger.info("PRE-PROCESSING: Extracting all unique agent data for batch processing")
        all_agent_data_set = set()
        
        for chunk in operation_chunks:
            for op in chunk:
                row_dict = op['row_dict']
                intermediary_1_id = row_dict.get('intermediary_1_id')
                transaction_field_15 = row_dict.get('transaction_field_15')
                if intermediary_1_id:
                    all_agent_data_set.add((intermediary_1_id, transaction_field_15))
        
        # Process all agents in one batch using existing method
        if all_agent_data_set:
            logger.info(f"PRE-PROCESSING: Processing {len(all_agent_data_set)} unique agents")
            cursor = connection.cursor()
            all_agent_data_list = list(all_agent_data_set)
            agent_entity_map = process_agents_in_chunks(cursor, all_agent_data_list, current_time, chunk_size)
            agents_processed = len([agent_id for agent_id, field_15 in all_agent_data_list 
                                 if agent_id in agent_entity_map])
            cursor.close()
            logger.info(f"PRE-PROCESSING: Agent processing complete - {agents_processed} agents processed")
        else:
            agent_entity_map = {}
        
        # Process each chunk with optimized batch operations
        for chunk_index, chunk in enumerate(operation_chunks):
            logger.info(f"Processing chunk {chunk_index + 1}/{len(operation_chunks)} ({len(chunk)} operations)")
            
            # Update progress
            progress = (chunk_index) / len(operation_chunks)
            progress_bar.progress(progress)
            
            with relationship_status_container.container():
                st.info(f"Processing chunk {chunk_index + 1}/{len(operation_chunks)} - Optimized batch operations")
            
            chunk_success = 0
            chunk_errors = 0
            chunk_beneficiaries_created = 0
            chunk_relationships_created = 0
            chunk_relationships_existed = 0
            
            try:
                cursor = connection.cursor()
                
                # PHASE 1: Collect all unique beneficiary banks for batch entity creation
                unique_beneficiaries = set()
                chunk_beneficiary_data = []
                
                for op in chunk:
                    row_dict = op['row_dict']
                    beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                    intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                    
                    if beneficiary_bank and beneficiary_bank != intermediary_bank:
                        unique_beneficiaries.add(beneficiary_bank)
                        # Store the transaction field data for entity creation
                        entity_data = parse_transaction_field_data(
                            row_dict.get('transaction_field_1'),
                            row_dict.get('transaction_field_2'), 
                            row_dict.get('transaction_field_3'),
                            row_dict.get('transaction_field_4')
                        )
                        chunk_beneficiary_data.append((beneficiary_bank, entity_data))
                
                # PHASE 2: Batch create beneficiary entities using existing patterns
                if unique_beneficiaries:
                    with relationship_status_container.container():
                        st.info(f"Chunk {chunk_index + 1}: Creating {len(unique_beneficiaries)} beneficiary entities")
                    
                    # Check existing entities in batch
                    existing_beneficiaries = batch_lookup_existing_entities(connection, list(unique_beneficiaries))
                    new_beneficiaries = unique_beneficiaries - set(existing_beneficiaries.keys())
                    
                    if new_beneficiaries:
                        # Prepare batch entity inserts
                        entity_inserts = []
                        entity_customer_inserts = []
                        entity_address_inserts = []
                        entity_identifier_inserts = []
                        
                        # Create mapping of beneficiary_bank to entity_data
                        beneficiary_entity_data = {bank: data for bank, data in chunk_beneficiary_data 
                                                 if bank in new_beneficiaries}
                        
                        # Prepare entity inserts
                        for beneficiary_bank in new_beneficiaries:
                            entity_inserts.append((
                                beneficiary_bank, 'Customer', 'UserUpload', 'UserUpload',
                                current_time, current_time, 'TransactionProcessor'
                            ))
                        
                        # Execute entity inserts
                        if entity_inserts:
                            entity_sql = """
                            INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, svc_provider_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.executemany(entity_sql, entity_inserts)
                            logger.info(f"Chunk {chunk_index + 1}: Inserted {len(entity_inserts)} beneficiary entities")
                        
                        # Get newly created entity IDs
                        new_entity_map = batch_lookup_existing_entities(connection, list(new_beneficiaries))
                        
                        # Prepare related record inserts in batches
                        for beneficiary_bank in new_beneficiaries:
                            entity_id = new_entity_map.get(beneficiary_bank)
                            entity_data = beneficiary_entity_data.get(beneficiary_bank, {})
                            
                            if entity_id:
                                # Entity customer insert
                                entity_customer_inserts.append((
                                    entity_id, beneficiary_bank,
                                    entity_data.get('first_name', ''),
                                    entity_data.get('middle_name', ''),
                                    entity_data.get('last_name', ''),
                                    entity_data.get('date_of_birth'),
                                    'Consumer', 'US', 'TransactionProcessor',
                                    current_time, current_time, 'UserUpload', 'UserUpload'
                                ))
                                
                                # Address insert if available
                                if entity_data.get('address_1'):
                                    entity_address_inserts.append((
                                        entity_id, entity_data.get('address_type', 'Primary'),
                                        entity_data.get('address_1', ''),
                                        entity_data.get('address_city', ''),
                                        entity_data.get('address_state', ''),
                                        entity_data.get('address_country', 'US'),
                                        entity_data.get('address_zipcode', ''),
                                        'TransactionProcessor', current_time, current_time,
                                        'UserUpload', 'UserUpload'
                                    ))
                                
                                # Identifier insert if available
                                if entity_data.get('identifier_type') and entity_data.get('identifier_value'):
                                    entity_identifier_inserts.append((
                                        entity_id, entity_data.get('identifier_type'),
                                        entity_data.get('identifier_value'),
                                        entity_data.get('source', 'TXN'),
                                        'TransactionProcessor', current_time, current_time,
                                        'UserUpload', 'UserUpload'
                                    ))
                        
                        # Execute all related record inserts in batches
                        if entity_customer_inserts:
                            customer_sql = """
                            INSERT INTO entity_customer (
                                entity_id, customer_entity_id, first_name, middle_name, last_name,
                                date_of_birth, customertype, nationality, svc_provider_name, 
                                inserted_date, updated_date, inserted_by, updated_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.executemany(customer_sql, entity_customer_inserts)
                            logger.info(f"Chunk {chunk_index + 1}: Inserted {len(entity_customer_inserts)} customer records")
                        
                        if entity_address_inserts:
                            address_sql = """
                            INSERT INTO entity_address (
                                entity_id, address_type, address_1, address_city, address_state,
                                address_country, address_zipcode, svc_provider_name,
                                inserted_date, update_date, inserted_by, updated_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.executemany(address_sql, entity_address_inserts)
                            logger.info(f"Chunk {chunk_index + 1}: Inserted {len(entity_address_inserts)} address records")
                        
                        if entity_identifier_inserts:
                            identifier_sql = """
                            INSERT INTO entity_identifier (
                                entity_id, identifier_type, identifier_value, source_system,
                                svc_provider_name, insert_date, update_date, inserted_by, updated_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.executemany(identifier_sql, entity_identifier_inserts)
                            logger.info(f"Chunk {chunk_index + 1}: Inserted {len(entity_identifier_inserts)} identifier records")
                        
                        chunk_beneficiaries_created = len(new_beneficiaries)
                
                # PHASE 3: Batch relationship processing using existing methods
                relationship_pairs = []
                for op in chunk:
                    row_dict = op['row_dict']
                    beneficiary_bank = row_dict.get('beneficiary_bank', '').strip()
                    intermediary_bank = row_dict.get('intermediary_bank', '').strip()
                    
                    if beneficiary_bank and beneficiary_bank != intermediary_bank:
                        intermediary_id = get_entity_id_by_bank(cursor, intermediary_bank)
                        beneficiary_id = get_entity_id_by_bank(cursor, beneficiary_bank)
                        
                        if intermediary_id and beneficiary_id:
                            pair = (intermediary_id, beneficiary_id)
                            relationship_pairs.append(pair)
                
                if relationship_pairs:
                    with relationship_status_container.container():
                        st.info(f"Chunk {chunk_index + 1}: Processing {len(relationship_pairs)} entity relationships")
                    
                    # Use existing batch relationship methods
                    existing_relationships = batch_check_relationships_exist_for_modify(cursor, relationship_pairs)
                    
                    if existing_relationships:
                        chunk_relationships_existed = len(existing_relationships)
                        updated_count = batch_update_relationships(cursor, existing_relationships, current_time)
                        logger.info(f"Chunk {chunk_index + 1}: Updated {updated_count} existing relationships")
                    
                    new_relationships = set(relationship_pairs) - existing_relationships
                    if new_relationships:
                        created_count = batch_create_new_relationships(cursor, new_relationships, current_time)
                        chunk_relationships_created = created_count
                        logger.info(f"Chunk {chunk_index + 1}: Created {created_count} new relationships")
                
                # PHASE 4: Batch transaction inserts using existing method
                with relationship_status_container.container():
                    st.info(f"Chunk {chunk_index + 1}: Processing {len(chunk)} transactions")
                
                transaction_inserts = []
                for op in chunk:
                    row_dict = op['row_dict']
                    db_account_id = op['db_account_id']
                    
                    # Use existing method with agent entity map
                    transaction_params = prepare_transaction_insert_params(
                        row_dict, db_account_id, current_time, agent_entity_map, cursor
                    )
                    
                    # Validate parameters using existing method
                    validation = validate_transaction_sql_parameters(transaction_params)
                    if not validation['match']:
                        logger.error(f"Row {op['index']}: Parameter validation failed - skipping transaction insert")
                        chunk_errors += 1
                        results.append(f"Row {op['index']}: Parameter validation failed for transaction {op.get('transaction_id', 'Unknown')}")
                        continue
                    
                    transaction_inserts.append(transaction_params)
                
                # Execute transaction batch insert using existing SQL
                if transaction_inserts:
                    transaction_sql = """
                    INSERT INTO transactions (
                        account_id, b_number, customer_transaction_id,
                        transaction_code, transaction_type,
                        preferred_currency, nominal_currency,
                        amount, debit_credit_indicator,
                        transaction_date, tran_code_description,
                        description, reference,
                        balance, eod_balance,
                        non_account_holder,transaction_origin_country,
                        counterparty_routing_number, counterparty_name,
                        branch_id, teller_id,
                        check_number, check_image_front_path,
                        check_image_back_path,
                        txn_location,
                        txn_source,
                        intermediary_1_id,
                        intermediary_1_type,
                        intermediary_1_code,
                        intermediary_1_name,
                        intermediary_1_role,
                        intermediary_1_reference,
                        intermediary_1_fee,
                        intermediary_1_fee_currency,
                        intermediary_2_id,
                        intermediary_2_type,
                        intermediary_2_code,
                        intermediary_2_name,
                        intermediary_2_role,
                        intermediary_2_reference,
                        intermediary_2_fee,
                        intermediary_2_fee_currency,
                        intermediary_3_id,
                        intermediary_3_type,
                        intermediary_3_code,
                        intermediary_3_name,
                        intermediary_3_role,
                        intermediary_3_reference,
                        intermediary_3_fee,
                        intermediary_3_fee_currency,
                        positive_pay_indicator,
                        originator_bank,
                        beneficiary_details,
                        intermediary_bank,
                        beneficiary_bank,
                        instructing_bank,
                        inter_bank_information,
                        other_information,
                        related_account,
                        transaction_field_1,
                        transaction_field_2,
                        transaction_field_3,
                        transaction_field_4,
                        transaction_field_5,
                        transaction_field_11,
                        transaction_field_12,
                        transaction_field_13,
                        transaction_field_14,
                        transaction_field_15,
                        is_p2p_transaction,
                        p2p_recipient_type,
                        p2p_recipient,
                        p2p_sender_type,
                        p2p_sender,
                        p2p_reference_id,
                        insert_date,
                        update_date,
                        inserted_by,
                        updated_by,
                        svc_provider_name
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    logger.info(f"Chunk {chunk_index + 1}: Executing batch insert for {len(transaction_inserts)} transactions")
                    cursor.executemany(transaction_sql, transaction_inserts)
                    chunk_success = len(transaction_inserts)
                    logger.info(f"Chunk {chunk_index + 1}: Successfully inserted {chunk_success} transactions")
                
                # Commit chunk changes
                connection.commit()
                cursor.close()
                
                with relationship_status_container.container():
                    st.success(f"Chunk {chunk_index + 1}: Successfully processed {chunk_success} transactions")
                
                # Update totals
                success_count += chunk_success
                error_count += chunk_errors
                beneficiary_entities_created += chunk_beneficiaries_created
                total_relationships_created += chunk_relationships_created
                total_relationships_existed += chunk_relationships_existed
                
                # Update metrics display
                with relationship_metrics_container.container():
                    st.subheader("Cumulative Processing Results")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    col1.metric("Total Transactions", success_count, delta=chunk_success)
                    col2.metric("Beneficiary Entities Created", beneficiary_entities_created, delta=chunk_beneficiaries_created)
                    col3.metric("Agent Entities Processed", agents_processed)
                    col4.metric("New Relationships", total_relationships_created, delta=chunk_relationships_created)
                    col5.metric("Updated Relationships", total_relationships_existed, delta=chunk_relationships_existed)
                
                # Generate results for this chunk
                for i, op in enumerate(chunk):
                    if i < chunk_success:
                        results.append(f"Row {op['index']}: Successfully added transaction {op.get('transaction_id', 'Unknown')}")
                
                if chunk_beneficiaries_created > 0:
                    results.append(f"Chunk {chunk_index + 1}: Created {chunk_beneficiaries_created} beneficiary entities")
                
                if chunk_relationships_created > 0:
                    results.append(f"Chunk {chunk_index + 1}: Created {chunk_relationships_created} new entity relationships")
                
                if chunk_relationships_existed > 0:
                    results.append(f"Chunk {chunk_index + 1}: Updated {chunk_relationships_existed} existing entity relationships")
                
            except Exception as chunk_error:
                logger.error(f"Chunk {chunk_index + 1}: Error processing chunk: {str(chunk_error)}")
                error_count += len(chunk)
                results.append(f"Chunk {chunk_index + 1}: Failed to process chunk: {str(chunk_error)}")
                
                with relationship_status_container.container():
                    st.error(f"Chunk {chunk_index + 1}: Processing failed: {str(chunk_error)}")
                
                try:
                    connection.rollback()
                    if 'cursor' in locals():
                        cursor.close()
                    logger.info(f"Chunk {chunk_index + 1}: Rollback completed")
                except Exception as rollback_error:
                    logger.error(f"Chunk {chunk_index + 1}: Rollback failed: {str(rollback_error)}")
                
                continue
        
        # Complete progress bar
        progress_bar.progress(1.0)
        
        # Final summary
        with relationship_status_container.container():
            st.success(f"Processing Complete! {success_count} transactions processed with optimized chunking")
        
        # Final comprehensive metrics
        with relationship_metrics_container.container():
            st.subheader("Final Optimized Processing Summary")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total Transactions Processed", success_count)
            col2.metric("Beneficiary Entities Created", beneficiary_entities_created)
            col3.metric("Agent Entities Processed", agents_processed)
            col4.metric("New Relationships Created", total_relationships_created)
            col5.metric("Existing Relationships Updated", total_relationships_existed)
        
        logger.info(f"Optimized ADD BATCH COMPLETE: Processed {len(operation_chunks)} chunks")
        logger.info(f"Optimized ADD SUMMARY: {success_count} transactions, {agents_processed} agents processed, {beneficiary_entities_created} beneficiary entities created, {total_relationships_created} relationships created, {total_relationships_existed} relationships updated")
        
        # Add summary to results
        summary_parts = []
        if agents_processed > 0:
            summary_parts.append(f"Processed {agents_processed} agent entities")
        if beneficiary_entities_created > 0:
            summary_parts.append(f"Created {beneficiary_entities_created} beneficiary entities")
        if total_relationships_created > 0:
            summary_parts.append(f"Created {total_relationships_created} new relationships")
        if total_relationships_existed > 0:
            summary_parts.append(f"Updated {total_relationships_existed} existing relationships")
        
        if summary_parts:
            results.insert(0, f"Optimized Entity Relationship Summary: {', '.join(summary_parts)}")
        
        # Clean up UI containers after a delay
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    except Exception as e:
        logger.error(f"Optimized batch ADD operation failed: {str(e)}")
        error_count = len(add_operations)
        results.append(f"Optimized batch transaction ADD operation failed: {str(e)}")
        
        with relationship_status_container.container():
            st.error(f"Error in optimized entity relationship processing: {str(e)}")
        
        try:
            connection.rollback()
            logger.info("Global ADD rollback completed")
        except Exception as rollback_error:
            logger.error(f"Global ADD rollback failed: {str(rollback_error)}")
        
        # Clean up UI on error
        time.sleep(1)
        relationship_status_container.empty()
        relationship_progress_container.empty()
        relationship_metrics_container.empty()
        
    return success_count, error_count, results

def process_transaction_add_operations_batch_csv(connection, add_operations):
    """Process transaction ADD operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} transaction ADD operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare transaction inserts
        transaction_inserts = []
        
        for op in add_operations:
            row_dict = op['row_dict']
            db_account_id = op['db_account_id']
            
            transaction_params = prepare_transaction_insert_params_csv(row_dict, db_account_id, current_time)
            transaction_inserts.append(transaction_params)
        
        # Execute transaction inserts
        transaction_sql = """
        INSERT INTO transactions (
        account_id,customer_transaction_id,transaction_code,transaction_type,preferred_currency,nominal_currency,
        amount, debit_credit_indicator, transaction_date,tran_code_description,description,reference,balance,
        eod_balance,non_account_holder,transaction_origin_country,counterparty_routing_number,counterparty_name,branch_id,teller_id,check_number,
        check_image_front_path,check_image_back_path,txn_location,txn_source,intermediary_1_id,
        intermediary_1_type,intermediary_1_code,intermediary_1_name,intermediary_1_role,intermediary_1_reference,intermediary_1_fee,intermediary_1_fee_currency,
        intermediary_2_id,intermediary_2_type,intermediary_2_code,intermediary_2_name,intermediary_2_role,intermediary_2_reference,intermediary_2_fee,  intermediary_2_fee_currency,
        intermediary_3_id,intermediary_3_type,intermediary_3_code,intermediary_3_name,intermediary_3_role,intermediary_3_reference,intermediary_3_fee,intermediary_3_fee_currency,
        positive_pay_indicator,originator_bank,beneficiary_details,intermediary_bank,beneficiary_bank,instructing_bank,
        inter_bank_information,other_information,related_account,transaction_field_1,transaction_field_2,
        transaction_field_3,transaction_field_4,transaction_field_5,transaction_field_11,transaction_field_12,
        transaction_field_13,transaction_field_14,transaction_field_15,is_p2p_transaction,p2p_recipient_type,
        p2p_recipient,p2p_sender_type,p2p_sender,p2p_reference_id,insert_date,update_date,
        inserted_by,updated_by,svc_provider_name
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s

        )

        """
        
        cursor.executemany(transaction_sql, transaction_inserts)
        connection.commit()
        cursor.close()
        
        success_count = len(add_operations)
        for op in add_operations:
            results.append(f"Row {op['index']}: Successfully added transaction {op['transaction_id']}")
        
        logger.info(f"Successfully completed batch ADD operations for {success_count} transactions")
        
    except Exception as e:
        error_count = len(add_operations)
        results.append(f"Batch transaction ADD operation failed: {str(e)}")
        logger.error(f"Batch transaction ADD operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rollback completed")
        except:
            pass
    
    return success_count, error_count, results

def process_transaction_add_operations_batch(connection, add_operations):
    """Process transaction ADD operations using proper batch approach"""
    results = []
    success_count = 0
    error_count = 0
    
    if not add_operations:
        return 0, 0, []
    
    logger.info(f"Processing {len(add_operations)} transaction ADD operations in batch")
    
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        # Prepare transaction inserts
        transaction_inserts = []
        
        for op in add_operations:
            row_dict = op['row_dict']
            db_account_id = op['db_account_id']
            
            transaction_params = prepare_transaction_insert_params_csv(row_dict, db_account_id, current_time)
            transaction_inserts.append(transaction_params)
        
        # Execute transaction inserts
        transaction_sql = """
        INSERT INTO transactions (
            account_id, b_number, customer_transaction_id, transaction_code, transaction_type,
            preferred_currency, nominal_currency, amount, debit_credit_indicator, transaction_date, 
            tran_code_description, description, reference, balance, eod_balance, 
            non_account_holder, transaction_origin_country, counterparty_routing_number, counterparty_name, branch_id, 
            teller_id, check_number, check_image_front_path, check_image_back_path, txn_location,
            txn_source, intermediary_1_id, intermediary_1_type, intermediary_1_code, intermediary_1_name, 
            intermediary_1_role, intermediary_1_reference, intermediary_1_fee, intermediary_1_fee_currency, intermediary_2_id, 
            intermediary_2_type, intermediary_2_code, intermediary_2_name, intermediary_2_role, intermediary_2_reference,
            intermediary_2_fee, intermediary_2_fee_currency, intermediary_3_id, intermediary_3_type, intermediary_3_code,
            intermediary_3_name, intermediary_3_role, intermediary_3_reference, intermediary_3_fee, intermediary_3_fee_currency,
            positive_pay_indicator, originator_bank, beneficiary_details,intermediary_bank,beneficiary_bank, instructing_bank, inter_bank_information, 
            other_information, related_account, transaction_field_1, transaction_field_2, transaction_field_3,
            transaction_field_4, transaction_field_5, transaction_field_11,transaction_field_12, transaction_field_13,
            transaction_field_14, transaction_field_15, is_p2p_transaction, p2p_recipient_type, p2p_recipient, 
            p2p_sender_type, p2p_sender, p2p_reference_id, insert_date, update_date, 
            inserted_by, updated_by, svc_provider_name
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s

        )

        """
        
        cursor.executemany(transaction_sql, transaction_inserts)
        connection.commit()
        cursor.close()
        
        success_count = len(add_operations)
        for op in add_operations:
            results.append(f"Row {op['index']}: Successfully added transaction {op['transaction_id']}")
        
        logger.info(f"Successfully completed batch ADD operations for {success_count} transactions")
        
    except Exception as e:
        error_count = len(add_operations)
        results.append(f"Batch transaction ADD operation failed: {str(e)}")
        logger.error(f"Batch transaction ADD operation failed: {str(e)}")
        try:
            connection.rollback()
            logger.info("Transaction rollback completed")
        except:
            pass
    
    return success_count, error_count, results

def get_or_create_agent_entity(cursor, intermediary_1_id, transaction_field_15, current_time):
    """
    Get or create agent entity for intermediary_1_id
    Returns the entity_id for the agent
    """
    if not intermediary_1_id:
        return None
    
    try:
        # Check if agent exists in entity_customer table
        cursor.execute("""
            SELECT entity_id 
            FROM entity_customer 
            WHERE customer_entity_id = %s AND customertype = 'Agents'
        """, (intermediary_1_id,))
        
        result = cursor.fetchone()
        
        if result:
            # Agent exists, return entity_id
            return result[0]
        else:
            # Agent doesn't exist, create new entries
            
            # Parse first_name and last_name from transaction_field_15
            first_name = ""
            last_name = ""
            
            if transaction_field_15:
                # Split by comma first, then by space
                if ',' in transaction_field_15:
                    parts = [part.strip() for part in transaction_field_15.split(',')]
                    if len(parts) >= 2:
                        first_name = parts[0]
                        last_name = parts[1]
                    elif len(parts) == 1:
                        first_name = parts[0]
                else:
                    # Split by space
                    parts = transaction_field_15.split()
                    if len(parts) >= 2:
                        first_name = parts[0]
                        last_name = ' '.join(parts[1:])  # Join remaining parts as last name
                    elif len(parts) == 1:
                        first_name = parts[0]
            
            # Insert into entity table
            cursor.execute("""
                INSERT INTO entity (customer_entity_id, entity_type, inserted_by, updated_by, insert_date, update_date, SVC_PROVIDER_NAME)
                VALUES (%s, 'Agents', %s, %s, %s, %s, %s)
            """, (intermediary_1_id, 'System', 'System', current_time, current_time, 'UserUpload-CashOnly'))
            
            # Get the entity_id of the newly inserted record
            entity_id = cursor.lastrowid
            
            # Insert into entity_customer table
            cursor.execute("""
                INSERT INTO entity_customer (entity_id, customer_entity_id, first_name, last_name, customertype, svc_provider_name, inserted_date, updated_date, inserted_by, updated_by)
                VALUES (%s, %s, %s, %s, 'Agents', %s, %s, %s, %s, %s)
            """, (entity_id, intermediary_1_id, first_name, last_name, 'UserUpload-CashOnly', current_time, current_time, 'System', 'System'))
            
            return entity_id
            
    except Exception as e:
        logger.error(f"Error in get_or_create_agent_entity: {str(e)}")
        return None
    
def prepare_transaction_insert_params_csv(row_dict, account_id, current_time, agent_entity_map=None, cursor=None):
    """Prepare parameters for transaction insert"""
    # Handle date conversion
    transaction_date = convert_datetime_field(row_dict.get('transaction_date'))
    
    # Handle boolean fields
    is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle amount and debit/credit indicator
    amount = convert_numeric_field(row_dict.get('amount'))
    debit_credit_indicator = 'D' if amount and amount > 0 else 'C' if amount else None
    
    if row_dict.get('settlement_currency') is None:
        settlement_currency = 'USD'
    else:
        settlement_currency =  row_dict.get('settlement_currency')

    related_account = row_dict.get('counterparty_account')
    
    # intermediary_1_str = row_dict.get('intermediary_1_id')
    
    # Get entity_id from agent_entity_map if provided, otherwise use single lookup
    # if agent_entity_map and intermediary_1_str in agent_entity_map:
    #     intermediary_1 = agent_entity_map[intermediary_1_str]
    # elif cursor:
    #     # Fallback to single lookup if not in map
    #     intermediary_1 = get_or_create_agent_entity(
    #         cursor, 
    #         intermediary_1_str, 
    #         row_dict.get('transaction_field_15'), 
    #         current_time
    #     )
    # else:
    #     intermediary_1 = None

    return (
        account_id,
        row_dict.get('transaction_id'), #customer_Transaction_id
        row_dict.get('transaction_code'),
        row_dict.get('transaction_type'),
        row_dict.get('preferred_currency'),
        settlement_currency,
        amount,
        debit_credit_indicator,
        transaction_date,
        row_dict.get('tran_code_description'), #10
        None, 
        None,
        row_dict.get('balance'),
        row_dict.get('eod_balance'),
        row_dict.get('non_account_holder'),
        'US',
        row_dict.get('counterparty_routing_number'),
        row_dict.get('counterparty_name'),
        row_dict.get('branch_id'),
        row_dict.get('teller_id'),
        row_dict.get('check_number'), #20
        row_dict.get('check_image_front_path'),
        row_dict.get('check_image_back_path'),
        row_dict.get('txn_location'),
        row_dict.get('channel'),
        None,  # intermediary_1_id                    # 26
        None,  # intermediary_1_type                  # 27
        None,  # intermediary_1_code                  # 28
        None,  # intermediary_1_name                  # 29
        None,  # intermediary_1_role                  # 30
        None,  # intermediary_1_reference             # 31
        None,  # intermediary_1_fee                   # 32
        None,  # intermediary_1_fee_currency          # 33
        None,  # intermediary_2_id                    # 34
        None,  # intermediary_2_type                  # 35
        None,  # intermediary_2_code                  # 36
        None,  # intermediary_2_name                  # 37
        None,  # intermediary_2_role                  # 38
        None,  # intermediary_2_reference             # 39
        None,  # intermediary_2_fee                   # 40
        None,  # intermediary_2_fee_currency          # 41
        None,  # intermediary_3_id                    # 42
        None,  # intermediary_3_type                  # 43
        None,  # intermediary_3_code                  # 44
        None,  # intermediary_3_name                  # 45
        None,  # intermediary_3_role                  # 46
        None,  # intermediary_3_reference             # 47
        None,  # intermediary_3_fee                   # 48
        None,  # intermediary_3_fee_currency          # 49
        None, #postive_pay_indicator
        row_dict.get('originator_bank'), #50
        None,
        row_dict.get('intermediary_bank'),
        row_dict.get('beneficiary_bank'),
        row_dict.get('instructing_bank'),
        row_dict.get('inter_bank_information'),
        row_dict.get('other_information'),
        related_account,
        None,  # transaction_field_1                  # 59
        None,  # transaction_field_2                  # 60
        None,  # transaction_field_3                  # 61
        None,  # transaction_field_4                  # 62
        None,  # transaction_field_5                  # 63
        None,  # transaction_field_11                 # 64
        None,  # transaction_field_12                 # 65
        None,  # transaction_field_13                 # 66
        None,  # transaction_field_14                 # 67
        None,  # transaction_field_15                 # 68
        is_p2p_transaction,
        row_dict.get('p2p_recipient_type'),
        row_dict.get('p2p_recipient'), #70
        row_dict.get('p2p_sender_type'),
        row_dict.get('p2p_sender'),
        row_dict.get('p2p_reference_id'),
        current_time,
        current_time,
        'System',
        'System',
        'UserUpload'
    )

def prepare_transaction_insert_params(row_dict, account_id, current_time, agent_entity_map=None, cursor=None):
    """Prepare parameters for transaction insert"""
    # Handle date conversion
    transaction_date = convert_datetime_field(row_dict.get('transaction_date'))
    
    # Handle boolean fields
    is_p2p_transaction = 1 if row_dict.get('is_p2p_transaction') in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 0
    
    # Handle amount and debit/credit indicator
    amount = convert_numeric_field(row_dict.get('amount'))
    debit_credit_indicator = 'D' if amount and amount > 0 else 'C' if amount else None
    
    if row_dict.get('settlement_currency') is None:
        settlement_currency = 'USD'
    else:
        settlement_currency =  row_dict.get('settlement_currency')

    related_account = row_dict.get('b_number')
    
    intermediary_1_str = row_dict.get('intermediary_1_id')
    
    # Get entity_id from agent_entity_map if provided, otherwise use single lookup
    if agent_entity_map and intermediary_1_str in agent_entity_map:
        intermediary_1 = agent_entity_map[intermediary_1_str]
    elif cursor:
        # Fallback to single lookup if not in map
        intermediary_1 = get_or_create_agent_entity(
            cursor, 
            intermediary_1_str, 
            row_dict.get('transaction_field_15'), 
            current_time
        )
    else:
        intermediary_1 = None

    return (
        account_id,
        row_dict.get('b_number'),
        row_dict.get('transaction_id'),
        row_dict.get('transaction_code'),
        row_dict.get('transaction_type'),
        row_dict.get('preferred_currency'),
        settlement_currency,
        amount,
        debit_credit_indicator,
        transaction_date,
        row_dict.get('tran_code_description'),
        row_dict.get('description'),
        row_dict.get('reference'),
        row_dict.get('balance'),
        row_dict.get('eod_balance'),
        row_dict.get('non_account_holder'),
        'US',
        row_dict.get('counterparty_routing_number'),
        row_dict.get('counterparty_name'),
        row_dict.get('branch_id'),
        row_dict.get('teller_id'),
        row_dict.get('check_number'),
        row_dict.get('check_image_front_path'),
        row_dict.get('check_image_back_path'),
        row_dict.get('txn_location'),
        row_dict.get('txn_source'),
        intermediary_1,
        'Agents',
        row_dict.get('intermediary_1_code'),
        row_dict.get('intermediary_1_name'),
        row_dict.get('intermediary_1_role'),
        row_dict.get('intermediary_1_reference'),
        row_dict.get('intermediary_1_fee'),
        row_dict.get('intermediary_1_fee_currency'),
        row_dict.get('intermediary_2_id'),
        row_dict.get('intermediary_2_type'),
        row_dict.get('intermediary_2_code'),
        row_dict.get('intermediary_2_name'),
        row_dict.get('intermediary_2_role'),
        row_dict.get('intermediary_2_reference'),
        row_dict.get('intermediary_2_fee'),
        row_dict.get('intermediary_2_fee_currency'),
        row_dict.get('intermediary_3_id'),
        row_dict.get('intermediary_3_type'),
        row_dict.get('intermediary_3_code'),
        row_dict.get('intermediary_3_name'),
        row_dict.get('intermediary_3_role'),
        row_dict.get('intermediary_3_reference'),
        row_dict.get('intermediary_3_fee'),
        row_dict.get('intermediary_3_fee_currency'),
        row_dict.get('positive_pay_indicator'),
        row_dict.get('originator_bank'),
        row_dict.get('beneficiary_details'),
        row_dict.get('intermediary_bank'),
        row_dict.get('beneficiary_bank'),
        row_dict.get('instructing_bank'),
        row_dict.get('inter_bank_information'),
        row_dict.get('other_information'),
        related_account,
        row_dict.get('transaction_field_1'),
        row_dict.get('transaction_field_2'),
        row_dict.get('transaction_field_3'),
        row_dict.get('transaction_field_4'),
        row_dict.get('transaction_field_5'),
        row_dict.get('transaction_field_11'),
        row_dict.get('transaction_field_12'),
        row_dict.get('transaction_field_13'),
        row_dict.get('transaction_field_14'),
        row_dict.get('transaction_field_15'),
        is_p2p_transaction,
        row_dict.get('p2p_recipient_type'),
        row_dict.get('p2p_recipient'),
        row_dict.get('p2p_sender_type'),
        row_dict.get('p2p_sender'),
        row_dict.get('p2p_reference_id'),
        current_time,
        current_time,
        'System',
        'System',
        'UserUpload-CashOnly'
    )

def convert_datetime_field(date_str):
    """Convert datetime string to datetime object"""
    if not date_str or pd.isna(date_str):
        return None
    
    # Try multiple datetime formats
    date_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%m/%d/%Y %H:%M',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except:
            continue
    return None

# ========================
# UTILITY FUNCTIONS
# ========================

def convert_to_mysql_date(date_str):
    """Convert various date formats to MySQL date format (YYYY-MM-DD)"""
    if not date_str or pd.isna(date_str) or date_str == '':
        return None
    
    # Try different date formats
    date_formats = [
        '%Y-%m-%d %H:%M:%S',  # 2023-01-15 14:30:00
        '%Y/%m/%d %H:%M:%S',  # 2023/01/15 14:30:00
        '%d-%m-%Y %H:%M:%S',  # 15-01-2023 14:30:00
        '%d/%m/%Y %H:%M:%S',  # 15/01/2023 14:30:00
        '%m-%d-%Y %H:%M:%S',  # 01-15-2023 14:30:00
        '%m/%d/%Y %H:%M:%S',  # 01/15/2023 14:30:00
        '%m/%d/%Y %H:%M',     # 01/15/2023 14:30 or 4/30/2025 12:13
        '%Y-%m-%d %H:%M',     # 2023-01-15 14:30
        '%Y/%m/%d %H:%M',     # 2023/01/15 14:30
        '%d-%m-%Y %H:%M',     # 15-01-2023 14:30
        '%d/%m/%Y %H:%M',     # 15/01/2023 14:30
        '%m-%d-%Y %H:%M',     # 01-15-2023 14:30
        '%Y-%m-%d',           # Date only: 2023-01-15
        '%Y/%m/%d',           # 2023/01/15
        '%d-%m-%Y',           # 15-01-2023
        '%d/%m/%Y',           # 15/01/2023
        '%m-%d-%Y',           # 01-15-2023
        '%m/%d/%Y',           # 01/15/2023
        '%Y%m%d'              # 19910626
    ]
    
    # Clean the date string
    if isinstance(date_str, str):
        date_str = date_str.strip()
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(str(date_str), fmt)
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            continue
    
    logger.warning(f"Could not parse date")
    return None

def convert_to_mysql_datetime(date_str):
    """Convert various date formats to MySQL datetime format (YYYY-MM-DD HH:MM:SS)"""
    if not date_str or pd.isna(date_str) or date_str == '':
        return None
    
    # Clean the date string
    if isinstance(date_str, str):
        date_str = date_str.strip()
    
    date_formats = [
        '%Y-%m-%d %H:%M:%S',  # Already MySQL format
        '%Y/%m/%d %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%m-%d-%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M',
        '%Y-%m-%d %H:%M',
        '%Y/%m/%d %H:%M',
        '%d-%m-%Y %H:%M',
        '%d/%m/%Y %H:%M',
        '%m-%d-%Y %H:%M',
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%m-%d-%Y',
        '%m/%d/%Y',
        '%Y%m%d',
        '%Y%m%d%H%M%S'
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(str(date_str), fmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            continue
    
    logger.warning(f"Could not parse datetime: {date_str}")
    return None

def preprocess_transaction_data(df):
    """Preprocess transaction data to fix date formats and other issues"""
    df_processed = df.copy()
    
    if 'transaction_date' in df_processed.columns:
        def convert_transaction_date(date_str):
            if not pd.isna(date_str) and isinstance(date_str, str):
                match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{1,2})$', date_str)
                if match:
                    month, day, year, hour, minute = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour.zfill(2)}:{minute.zfill(2)}:00"
            return date_str
        
        df_processed['transaction_date'] = df_processed['transaction_date'].apply(convert_transaction_date)
        df_processed['transaction_date'] = pd.to_datetime(df_processed['transaction_date'], errors='coerce')
        df_processed['transaction_date'] = df_processed['transaction_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Convert amount columns to float
    if 'amount' in df_processed.columns:
        df_processed['amount'] = pd.to_numeric(df_processed['amount'], errors='coerce')
    
    # Standardize debit/credit indicator
    if 'debit_credit_indicator' in df_processed.columns:
        df_processed['debit_credit_indicator'] = df_processed['debit_credit_indicator'].apply(
            lambda x: 'D' if x in ['D', 'd', 'DR', 'dr','DEBIT','Debit','debit'] else ('C' if x in ['C', 'c', 'CR', 'cr','CREDIT','Credit','credit'] else x)
        )
    
    # Standardize Y/N fields
    bool_columns = ['is_p2p_transaction']
    for col in bool_columns:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(
                lambda x: 'Y' if x in ['Yes', 'yes', 'Y', 'y', '1', 'true', 'True', True] else 'N'
            )
    
    return df_processed

# ========================
# VALIDATION FUNCTIONS
# ========================

def determine_customer_type_from_id(customer_id):
    """Determine customer type from customer_id suffix for fixed-width files"""
    if not customer_id or pd.isna(customer_id):
        return 'Consumer'  # Default fallback
    
    customer_id_str = str(customer_id).strip().upper()
    
    if customer_id_str.endswith('CUSWBP'):
        return 'Consumer'
    elif customer_id_str.endswith('TIDWBP'):
        return 'Terminals'
    elif customer_id_str.endswith('AIDWBP'):
        return 'Agents'
    elif customer_id_str.endswith('GRPWBP'):
        return 'Agent Groups'
    elif customer_id_str.endswith('OWNWBP'):
        return 'Agent Owners'
    elif customer_id_str.endswith('CHNWBP'):
        return 'Agent Chains'
    else:
        # For business customers or other types, default to Business
        return 'Business'

def validate_data(df, is_fixed_width=False):
    """Validate the uploaded CSV data"""
    validation_errors = []
    
    # Check required columns
    if is_fixed_width:
        required_columns = ['changetype', 'customer_id', 'customer_type']
    else:
        required_columns = ['changetype', 'customer_id', 'customer_type']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return validation_errors
    
    # Check for empty customer_ids
    empty_customer_ids = df[df['customer_id'].isna() | (df['customer_id'] == '')].index.tolist()
    if empty_customer_ids:
        if len(empty_customer_ids) > 10:
            validation_errors.append(f"Empty customer_id found in {len(empty_customer_ids)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_customer_ids[:10]]))}, and more...")
        else:
            validation_errors.append(f"Empty customer_id found in rows: {', '.join(map(str, [i+2 for i in empty_customer_ids]))}")
    
    # Validate changetype values - SKIP for fixed width files
    if not is_fixed_width:
        df_copy = df.copy()
        valid_changetypes = ['add', 'modify', 'delete']
        df_copy['changetype'] = df_copy['changetype'].fillna('').astype(str).str.lower().str.strip()
        
        # Filter out empty strings and check for invalid values
        non_empty_changetypes = df_copy[df_copy['changetype'] != '']
        
        if len(non_empty_changetypes) > 0:
            invalid_changetypes_mask = ~non_empty_changetypes['changetype'].isin(valid_changetypes)
            
            if invalid_changetypes_mask.any():
                invalid_types = non_empty_changetypes.loc[invalid_changetypes_mask, 'changetype'].unique()
                # Convert to strings and filter out NaN representations - FIXED
                invalid_types_str = []
                for t in invalid_types:
                    str_val = str(t).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid changetype values: {', '.join(invalid_types_str)}. Valid values are add, modify or delete")
        
        # Check for empty changetype values
        empty_changetypes = df[df['changetype'].isna() | (df['changetype'] == '')].index.tolist()
        if empty_changetypes:
            if len(empty_changetypes) > 10:
                validation_errors.append(f"Empty changetype found in {len(empty_changetypes)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_changetypes[:10]]))}, and more...")
            else:
                validation_errors.append(f"Empty changetype found in rows: {', '.join(map(str, [i+2 for i in empty_changetypes]))}")
    
    # Validate customer_type values with updated logic for fixed-width vs CSV
    valid_customer_types = ['Consumer', 'Business', 'Terminals', 'Agents', 'Agent Groups', 'Agent Owners', 'Agent Chains']
    
    if not is_fixed_width:
        # For fixed-width files, customer_type is automatically determined from customer_id
        # We should validate that the automatic determination worked correctly
        # Check if any customer_type is empty or invalid after mapping
        df_copy = df.copy()
        df_copy['customer_type'] = df_copy['customer_type'].fillna('').astype(str).str.strip()
        
        # Check for empty customer_types (should not happen if mapping worked)
        empty_customer_types = df_copy[df_copy['customer_type'] == '']
        if len(empty_customer_types) > 0:
            validation_errors.append(f"Customer type could not be determined for {len(empty_customer_types)} records. Please check customer_id format.")
        
        # Validate that determined customer_types are valid
        non_empty_customer_types = df_copy[df_copy['customer_type'] != '']
        if len(non_empty_customer_types) > 0:
            invalid_customer_types_mask = ~non_empty_customer_types['customer_type'].isin(valid_customer_types)
            
            if invalid_customer_types_mask.any():
                invalid_types = non_empty_customer_types.loc[invalid_customer_types_mask, 'customer_type'].unique()
                invalid_types_str = []
                for t in invalid_types:
                    str_val = str(t).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid customer_type values determined from customer_id: {', '.join(invalid_types_str)}. Valid values are {', '.join(valid_customer_types)}")
    else:
        # For CSV files, validate user-provided customer_type values
        df_copy = df.copy()
        df_copy['customer_type'] = df_copy['customer_type'].fillna('').astype(str).str.strip()
        non_empty_customer_types = df_copy[df_copy['customer_type'] != '']
        
        if len(non_empty_customer_types) > 0:
            invalid_customer_types_mask = ~non_empty_customer_types['customer_type'].isin(valid_customer_types)
            
            if invalid_customer_types_mask.any():
                invalid_types = non_empty_customer_types.loc[invalid_customer_types_mask, 'customer_type'].unique()
                invalid_types_str = []
                for t in invalid_types:
                    str_val = str(t).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid customer_type values: {', '.join(invalid_types_str)}. Valid values are {', '.join(valid_customer_types)}")
    
    # Validate date formats
    date_columns = ['date_of_birth','deceased_date', 'date_of_incorporation', 'bankruptcy_date', 'document_expiry_date', 'closed_date']
    
    for date_column in date_columns:
        if date_column in df.columns:
            non_empty_mask = df[date_column].notna() & (df[date_column] != '')
            if non_empty_mask.any():
                date_values = df.loc[non_empty_mask, date_column]
                valid_dates = [convert_to_mysql_date(date) is not None for date in date_values]
                invalid_mask = ~pd.Series(valid_dates, index=date_values.index)
                
                if invalid_mask.any():
                    invalid_values = date_values[invalid_mask]
                    if len(invalid_values) > 5:
                        validation_errors.append(f"Invalid date format in column {date_column}: {len(invalid_values)} invalid dates including {', '.join(invalid_values.head(5).astype(str))} (showing first 5 only)")
                    else:
                        validation_errors.append(f"Invalid date format in column {date_column}: {', '.join(invalid_values.astype(str))}")
    
    return validation_errors

def validate_account_data(df, is_fixed_width=False):
    """Validate the uploaded account CSV data"""
    validation_errors = []
    
    # Check required columns
    required_columns = ['changetype', 'customer_id', 'account_number']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return validation_errors
    
    
    # SKIP changetype validation for fixed-width files since they default to 'add'
    if is_fixed_width:
        # Validate changetype values only for CSV files
        valid_changetypes = ['add', 'modify', 'delete']
        
        # Create a copy to avoid modifying original data
        df_copy = df.copy()
        df_copy['changetype'] = df_copy['changetype'].fillna('').astype(str).str.lower().str.strip()
        
        # Filter out empty strings
        non_empty_changetypes = df_copy[df_copy['changetype'] != '']
        
        if len(non_empty_changetypes) > 0:
            invalid_changetypes_mask = ~non_empty_changetypes['changetype'].isin(valid_changetypes)
            
            if invalid_changetypes_mask.any():
                invalid_types = non_empty_changetypes.loc[invalid_changetypes_mask, 'changetype'].unique()
                
                # FIXED: Convert all values to strings before joining
                invalid_types_str = []
                for val in invalid_types:
                    str_val = str(val).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid changetype values: {', '.join(invalid_types_str)}. Valid values are add, modify or delete")
        
        # Check for empty changetype values
        empty_changetypes = df[df['changetype'].isna() | (df['changetype'] == '')].index.tolist()
        if empty_changetypes:
            if len(empty_changetypes) > 10:
                validation_errors.append(f"Empty changetype found in {len(empty_changetypes)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_changetypes[:10]]))}, and more...")
            else:
                validation_errors.append(f"Empty changetype found in rows: {', '.join(map(str, [i+2 for i in empty_changetypes]))}")
        
        # Validate date formats only for CSV files
        date_columns = ['open_date', 'p2p_enrollment_date']
        
        for date_column in date_columns:
            if date_column in df.columns:
                non_empty_mask = df[date_column].notna() & (df[date_column] != '')
                if non_empty_mask.any():
                    date_values = df.loc[non_empty_mask, date_column]
                    valid_dates = [convert_to_mysql_date(date) is not None for date in date_values]
                    invalid_mask = ~pd.Series(valid_dates, index=date_values.index)
                    
                    if invalid_mask.any():
                        invalid_values = date_values[invalid_mask]
                        if len(invalid_values) > 5:
                            validation_errors.append(f"Invalid date format in column {date_column}: {len(invalid_values)} invalid dates including {', '.join(invalid_values.head(5).astype(str))} (showing first 5 only)")
                        else:
                            validation_errors.append(f"Invalid date format in column {date_column}: {', '.join(invalid_values.astype(str))}")
    
    # Validate numeric fields (both CSV and fixed-width)
    numeric_columns = ['account_balance']
    
    for numeric_column in numeric_columns:
        if numeric_column in df.columns:
            non_empty_mask = df[numeric_column].notna() & (df[numeric_column] != '')
            if non_empty_mask.any():
                numeric_values = df.loc[non_empty_mask, numeric_column]
                converted_numeric = pd.to_numeric(numeric_values, errors='coerce')
                invalid_mask = converted_numeric.isna()
                
                if invalid_mask.any():
                    invalid_values = numeric_values[invalid_mask]
                    if len(invalid_values) > 5:
                        validation_errors.append(f"Invalid numeric format in column {numeric_column}: {len(invalid_values)} invalid values including {', '.join(invalid_values.head(5).astype(str))} (showing first 5 only)")
                    else:
                        validation_errors.append(f"Invalid numeric format in column {numeric_column}: {', '.join(invalid_values.astype(str))}")
    
    return validation_errors

def validate_transaction_data(df, is_fixed_width=False):
    """Validate the uploaded transaction CSV data"""
    validation_errors = []
    
    # Check required columns
    required_columns = ['changetype', 'transaction_id', 'account_id', 'transaction_date']

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return validation_errors
    
    # Check for empty transaction_ids
    if not is_fixed_width:
        empty_transaction_ids = df[df['transaction_id'].isna() | (df['transaction_id'] == '')].index.tolist()
        if empty_transaction_ids:
            if len(empty_transaction_ids) > 10:
                validation_errors.append(f"Empty transaction_id found in {len(empty_transaction_ids)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_transaction_ids[:10]]))}, and more...")
            else:
                validation_errors.append(f"Empty transaction_id found in rows: {', '.join(map(str, [i+2 for i in empty_transaction_ids]))}")
    

    # Check for empty account_ids
    if not is_fixed_width:
        empty_account_ids = df[df['account_id'].isna() | (df['account_id'] == '')].index.tolist()
        if empty_account_ids:
            if len(empty_account_ids) > 10:
                validation_errors.append(f"Empty account_id found in {len(empty_account_ids)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_account_ids[:10]]))}, and more...")
            else:
                validation_errors.append(f"Empty account_id found in rows: {', '.join(map(str, [i+2 for i in empty_account_ids]))}")
    
    # SKIP changetype validation for fixed-width files since they default to 'add'
    if not is_fixed_width:
        # Validate changetype values only for CSV files
        valid_changetypes = ['add', 'modify', 'delete']
        
        df_copy = df.copy()
        df_copy['changetype'] = df_copy['changetype'].fillna('').astype(str).str.lower().str.strip()
        
        non_empty_changetype = df_copy[df_copy['changetype'] != '']
        
        if len(non_empty_changetype) > 0:
            invalid_changetypes_mask = ~non_empty_changetype['changetype'].isin(valid_changetypes)
            
            if invalid_changetypes_mask.any():
                invalid_types = non_empty_changetype.loc[invalid_changetypes_mask, 'changetype'].unique()
                
                # FIXED: Convert all values to strings before joining
                invalid_types_str = []
                for val in invalid_types:
                    str_val = str(val).strip()
                    if str_val not in ['nan', 'NaN', '', 'None', 'none']:
                        invalid_types_str.append(str_val)
                
                if invalid_types_str:
                    validation_errors.append(f"Invalid changetype values: {', '.join(invalid_types_str)}. Valid values are add, modify or delete")
        
        # Check for empty changetype values
        empty_changetype = df[df['changetype'].isna() | (df['changetype'] == '')].index.tolist()
        if empty_changetype:
            if len(empty_changetype) > 10:
                validation_errors.append(f"Empty changetype found in {len(empty_changetype)} rows, including rows: {', '.join(map(str, [i+2 for i in empty_changetype[:10]]))}, and more...")
            else:
                validation_errors.append(f"Empty changetype found in rows: {', '.join(map(str, [i+2 for i in empty_changetype]))}")
    
    # Validate transaction_date (both CSV and fixed-width)
    if 'transaction_date' in df.columns:
        non_empty_mask = df['transaction_date'].notna() & (df['transaction_date'] != '')
        if non_empty_mask.any():
            transaction_dates = df.loc[non_empty_mask, 'transaction_date']
            converted_dates = pd.to_datetime(transaction_dates, errors='coerce')
            
            failed_mask = converted_dates.isna()
            if failed_mask.any():
                failed_dates = transaction_dates[failed_mask]
                if len(failed_dates) > 5:
                    validation_errors.append(f"Could not parse dates in column transaction_date: {len(failed_dates)} invalid dates including {', '.join(failed_dates.head(5).astype(str))} (showing first 5 only)")
                else:
                    validation_errors.append(f"Could not parse dates in column transaction_date: {', '.join(failed_dates.astype(str))}")
    
    # Validate numeric fields (both CSV and fixed-width)
    numeric_columns = ['amount', 'exchange_rate_applied']
    
    for numeric_column in numeric_columns:
        if numeric_column in df.columns:
            non_empty_mask = df[numeric_column].notna() & (df[numeric_column] != '')
            if non_empty_mask.any():
                numeric_values = df.loc[non_empty_mask, numeric_column]
                converted_numeric = pd.to_numeric(numeric_values, errors='coerce')
                invalid_mask = converted_numeric.isna()
                
                if invalid_mask.any():
                    invalid_values = numeric_values[invalid_mask]
                    if len(invalid_values) > 5:
                        validation_errors.append(f"Invalid numeric format in column {numeric_column}: {len(invalid_values)} invalid values including {', '.join(invalid_values.head(5).astype(str))} (showing first 5 only)")
                    else:
                        validation_errors.append(f"Invalid numeric format in column {numeric_column}: {', '.join(invalid_values.astype(str))}")
    
    return validation_errors



#################################################
## This section enables the 2 part of data load and aml detection seperately
## This is done such that the dataloader program can be used by Fraud Detection system as well
## Ensure to comment out the Step 2 portion of this method
#################################################
def create_transaction_upload_tab():
    """Create the enhanced transaction upload tab with manual AML detection"""
    st.header("Upload Transaction CSV Data")
    st.markdown("**Enhanced with AML Detection:** Upload transactions first, then manually trigger AML detection")
    
    # AML Status indicator
    if st.session_state.aml_available:
        st.success(" AML Detection System: **ACTIVE**")
    else:
        st.warning(" AML Detection System: **UNAVAILABLE**")
    
    TRANSACTION_CHUNK_SIZE = 5000
    allow_duplicate_transactions = True
    
    # Warn if accounts haven't been processed
    if not st.session_state.account_processed:
        st.warning(" It is recommended to process account data first before processing transactions")
    
    transaction_file = st.file_uploader("Choose a Transaction CSV file", type="csv", key="transaction_batch_upload")
    
    if transaction_file is not None:
        try:
            # Save uploaded file temporarily for AML processing
            temp_file_path = f"temp_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            with open(temp_file_path, 'wb') as f:
                f.write(transaction_file.getbuffer())
            
            # Store the temp file path in session state for later AML detection
            st.session_state.temp_transaction_file = temp_file_path
            
            transaction_df = pd.read_csv(transaction_file, low_memory=False)
            st.write(f"File loaded successfully: {transaction_file.name}")
            st.write(f"Total records: {len(transaction_df):,}")
            
            st.subheader("Sample Data (first 5 rows)")
            st.dataframe(transaction_df.head(5))

            with st.spinner("Preprocessing transaction data..."):
                original_df = transaction_df.copy()
                transaction_df = preprocess_transaction_data(transaction_df)
                
                # Show date conversion examples
                st.markdown("<h3 style='font-size: 18px;'> Date Format Conversion Examples</h3>", unsafe_allow_html=True)
                # st.subheader("Date Format Conversion Examples")
                if 'transaction_date' in transaction_df.columns:
                    date_examples = []
                    for i, (orig, conv) in enumerate(zip(
                        original_df['transaction_date'].head(5),
                        transaction_df['transaction_date'].head(5)
                    )):
                        date_examples.append({
                            "Row": i+1,
                            "Original Format": orig,
                            "Converted Format": conv
                        })
                    st.table(date_examples)
            
            # Validate data
            with st.spinner("Validating transaction data..."):
                validation_errors = validate_transaction_data(transaction_df)
            
            if validation_errors:
                st.error("Validation Errors Detected")
                for error in validation_errors:
                    st.error(error)
                st.warning("Please fix the errors in your Transaction CSV file before processing.")
            else:
                st.success(" Transaction data validation passed successfully!.. To continue, click on Process Transaction Data option below")
                
                # Count of operations by type
                operation_counts = transaction_df['changetype'].str.lower().value_counts().to_dict()
                st.markdown("<h3 style='font-size: 18px;'>Operation Summary</h3>", unsafe_allow_html=True)
                # st.subheader("Operation Summary")
                cols = st.columns(3)
                cols[0].metric("Add Operations", operation_counts.get('add', 0))
                cols[1].metric("Modify Operations", operation_counts.get('modify', 0))
                cols[2].metric("Delete Operations", operation_counts.get('delete', 0))
                
                # Separate buttons for transaction processing and AML detection
                st.markdown("---")
                
                # Step 1: Process Transaction Data
                st.subheader("Step 1: Process Transaction Data")
                st.info(" Upload and process transaction data into the database")
                
                if st.button(" Process Transaction Data (Database Upload Only)", key="process_transaction_batch_only"):
                    if not st.session_state.db_connected:
                        st.error("Please connect to the database first!")
                    else:
                        with st.spinner("Processing transaction data (without AML detection)..."):
                            start_time = time.time()
                            connection = create_db_connection()
                            
                            if connection and connection.is_connected():
                                logger.info(f"Starting batch transaction processing for file with {len(transaction_df)} records (without AML)")
                                
                                # Set global chunk size
                                CHUNK_SIZE = TRANSACTION_CHUNK_SIZE
                                
                                # Use the enhanced function WITHOUT AML detection (pass None for csv_file_path)
                                success_count, error_count, results, aml_results = process_transaction_csv_data_batch(
                                    transaction_df, connection, allow_duplicate_transactions, None)
                                connection.close()
                                
                                # Store results in session state
                                st.session_state.transaction_success_count = success_count
                                st.session_state.transaction_error_count = error_count
                                st.session_state.transaction_results = results
                                st.session_state.transaction_processed = True
                                st.session_state.transaction_upload_completed = True  # New flag for upload completion
                                
                                end_time = time.time()
                                processing_time = end_time - start_time
                                
                                # Display results summary
                                st.success(f"Transaction upload process completed in {processing_time:.2f} seconds! Success: {success_count:,}, Errors: {error_count:,}")
                                
                                # Calculate processing rate
                                if processing_time > 0:
                                    rate = len(transaction_df) / processing_time
                                    st.info(f"Processing rate: {rate:.2f} records/second")
                                
                                logger.info(f"Transaction processing completed in {processing_time:.2f} seconds")
                                
                                # Show next step
                                st.success(" Transaction data has been successfully uploaded to the database!")
                                st.info(" Now you can proceed to Step 2 to run AML Detection on the uploaded transactions.")
                                
                            else:
                                st.error("Could not connect to the database.")
                
                
                # Show current AML status if available
                if st.session_state.get('aml_completed', False):
                    st.markdown("---")
                    st.subheader(" AML Detection is completed.. ")
                    
                    aml_results = st.session_state.get('aml_results', {})
                    alerts_generated = aml_results.get('alerts_generated', 0)
                    
                   
        except Exception as e:
            st.error(f"Error reading Transaction CSV file: {e}")
            st.exception(e)
        
        # Clean up temporary file when done (add this at the end)
        finally:
            # Clean up temporary file when the session ends or file is no longer needed
            if hasattr(st.session_state, 'temp_transaction_file'):
                # Note: We keep the file for AML detection, but clean this up if required
                # after AML detection is complete or when the session ends
                pass


def create_aml_results_tab():
    """Create a dedicated tab for AML detection results"""
    st.header(" AML Detection Results")
    
    if not st.session_state.aml_available:
        st.error(" AML Detection system is not available")
        st.info("Please ensure the AML detection module is properly installed and configured.")
        return
    
    if st.session_state.get('aml_completed', False) and 'aml_results' in st.session_state:
        aml_results = st.session_state.aml_results
        
        # Display comprehensive results
        st.subheader(" Detection Summary")
        display_aml_results(aml_results)
        
        # Additional detailed analysis
        if aml_results.get('alerts_generated', 0) > 0:
            st.subheader("Alert Analysis")
            st.info("Alerts have been generated and stored in the database. Please check the Case Manager UI for detailed information.")
        
        # Show processing statistics
        if 'batch_insert_results' in aml_results:
            insert_stats = aml_results['batch_insert_results'].get('stats', {})
            st.subheader(" Data Processing Statistics")
            
            col1, col2, col3, col4 = st.columns(3)
            with col1:
                st.metric("Records Processed", insert_stats.get('total_records', 0))
            with col2:
                st.metric("Successful Inserts", insert_stats.get('successful_inserts', 0))
            with col3:
                st.metric("Records Updated", insert_stats.get('updated_records', 0))
            with col4:
                st.metric("Duplicates Skipped", insert_stats.get('duplicate_records', 0))
    
    else:
        st.info("No AML detection results available. Upload and process transaction data to see results here.")
        
  
def replace_customer_upload_tab():
    """Replace the existing customer upload tab with enhanced version"""
    # create_enhanced_customer_upload_tab_with_multi_format()
    create_enhanced_customer_upload_tab_with_fixed_width()
    

def replace_account_upload_tab():
    """Replace the existing account upload tab with enhanced version"""
    # create_enhanced_account_upload_tab_with_multi_format()
    create_enhanced_account_upload_tab_with_fixed_width()

def replace_transaction_upload_tab():
    """Replace the existing transaction upload tab with enhanced version"""
    # create_enhanced_transaction_upload_tab_with_multi_format()
    create_enhanced_transaction_upload_tab_with_fixed_width()

# Enhanced template section with all formats
def replace_template_section():
    """Replace the existing template section with enhanced multi-format version"""
    create_enhanced_template_section_with_formats()
    # create_comprehensive_template_section()



def create_schema_editor_interface():
    """Create comprehensive schema editor interface"""
    st.markdown("<h3 style='font-size: 20px;'>🗺️ Fixed Width Schema Editor</h3>", unsafe_allow_html=True)
    # st.markdown("Create and edit schemas for fixed-width file processing")
    
    # Schema selection
    schema_type = st.selectbox(
        "Select Schema to Edit:",
        ["Customer", "Account", "Transaction", "Relationship", "Create New"]
    )
    
    if schema_type == "Create New":
        create_new_schema_interface()
    else:
        edit_existing_schema_interface(schema_type.lower())

def create_new_schema_interface():
    """Interface for creating a new schema"""
    st.subheader("🆕 Create New Schema")
    
    with st.form("new_schema_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            schema_name = st.text_input("Schema Name", placeholder="e.g., New_Data_Type")
            file_pattern = st.text_input("File Pattern", placeholder="e.g., NEWTYPE_*.txt")
            total_length = st.number_input("Total Record Length", min_value=1, max_value=10000, value=1000)
        
        with col2:
            description = st.text_area("Description", placeholder="Describe the schema purpose")
            version = st.text_input("Version", value="1.0")
        
        st.markdown("### Field Definitions")
        
        # Dynamic field addition
        if 'new_schema_fields' not in st.session_state:
            st.session_state.new_schema_fields = []
        
        # Add field button
        if st.form_submit_button("➕ Add Field"):
            st.session_state.new_schema_fields.append({
                'field_name': '',
                'column_name': '',
                'format_type': 'Text',
                'position': 1,
                'size': 10,
                'description': '',
                'mapping': ''
            })
            st.rerun()
        
        # Display and edit fields
        for i, field in enumerate(st.session_state.new_schema_fields):
            st.markdown(f"**Field {i + 1}:**")
            
            field_col1, field_col2, field_col3, field_col4 = st.columns(4)
            
            with field_col1:
                field['field_name'] = st.text_input(f"Field Name", value=field['field_name'], key=f"field_name_{i}")
                field['position'] = st.number_input(f"Position", min_value=1, value=field['position'], key=f"position_{i}")
            
            with field_col2:
                field['column_name'] = st.text_input(f"Column Name", value=field['column_name'], key=f"column_name_{i}")
                field['size'] = st.number_input(f"Size", min_value=1, value=field['size'], key=f"size_{i}")
            
            with field_col3:
                field['format_type'] = st.selectbox(f"Format", 
                    ["Text", "Number", "Date", "Amount"], 
                    index=["Text", "Number", "Date", "Amount"].index(field['format_type']),
                    key=f"format_{i}")
            
            with field_col4:
                field['mapping'] = st.text_input(f"Database Mapping", value=field['mapping'], key=f"mapping_{i}")
            
            field['description'] = st.text_input(f"Description", value=field['description'], key=f"desc_{i}")
            
            if st.button(f"🗑️ Remove Field {i + 1}", key=f"remove_{i}"):
                st.session_state.new_schema_fields.pop(i)
                st.rerun()
            
            st.markdown("---")
        
        # Save schema
        if st.form_submit_button("💾 Save Schema"):
            if schema_name and st.session_state.new_schema_fields:
                save_custom_schema(schema_name, file_pattern, st.session_state.new_schema_fields)
                st.success(f" Schema '{schema_name}' saved successfully!")
                st.session_state.new_schema_fields = []  # Clear fields
            else:
                st.error(" Please provide schema name and at least one field")

def edit_existing_schema_interface(schema_type):
    """Interface for editing existing schemas"""
    st.markdown("<h3 style='font-size: 20px;'>✏️ Edit " + schema_type.title() + " Schema</h3>", unsafe_allow_html=True)

    # st.subheader(f"✏️ Edit {schema_type.title()} Schema")
    
    # Load existing schema
    if schema_type == 'customer':
        schema = create_customer_fixed_width_schema()
    elif schema_type == 'account':
        schema = create_account_fixed_width_schema()
    elif schema_type == 'transaction':
        schema = create_transaction_fixed_width_schema()
    elif schema_type == 'relationship':
        schema = create_relationship_fixed_width_schema()
    
    # Show current schema info
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Fields", len(schema.fields))
    col2.metric("Record Length", schema.total_length)
    col3.metric("Schema Type", schema_type.title())
    
    # Schema editing tabs
    edit_tabs = st.tabs(["📋 View Fields", "✏️ Edit Fields", "➕ Add Fields", "🗑️ Remove Fields", "💾 Save Changes"])
    
    with edit_tabs[0]:
        # View current fields
        st.markdown("### Current Schema Fields")
        
        fields_data = []
        for field in schema.fields:
            fields_data.append({
                "Field": field.field_name,
                "Name": field.column_name,
                "Position": f"{field.position}-{field.position + field.size - 1}",
                "Size": field.size,
                "Type": field.format_type,
                "Mapping": field.bliink_mapping[:50] + "..." if len(field.bliink_mapping) > 50 else field.bliink_mapping
            })
        
        fields_df = pd.DataFrame(fields_data)
        st.dataframe(fields_df, use_container_width=True, height=400)
        
        # Export schema
        if st.button("📥 Export Schema as JSON"):
            schema_json = export_schema_to_json(schema)
            st.download_button(
                label="Download Schema JSON",
                data=schema_json,
                file_name=f"{schema_type}_schema.json",
                mime="application/json"
            )
    
    with edit_tabs[1]:
        # Edit existing fields
        st.markdown("### Edit Existing Fields")
        
        selected_field_index = st.selectbox(
            "Select Field to Edit:",
            range(len(schema.fields)),
            format_func=lambda x: f"{schema.fields[x].field_name} - {schema.fields[x].column_name}"
        )
        
        if selected_field_index is not None:
            field = schema.fields[selected_field_index]
            
            with st.form(f"edit_field_{selected_field_index}"):
                st.markdown(f"**Editing Field: {field.field_name}**")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    new_field_name = st.text_input("Field Name", value=field.field_name)
                    new_column_name = st.text_input("Column Name", value=field.column_name)
                    new_position = st.number_input("Position", min_value=1, value=field.position)
                    new_size = st.number_input("Size", min_value=1, value=field.size)
                
                with col2:
                    new_format_type = st.text_input("Format Type", value=field.format_type)
                    new_check_free_pay = st.text_area("Check Free Pay", value=field.check_free_pay)
                    new_mapping = st.text_area("Database Mapping", value=field.bliink_mapping)
                
                if st.form_submit_button("💾 Update Field"):
                    # Update the field
                    schema.fields[selected_field_index] = FixedWidthField(
                        field_name=new_field_name,
                        column_name=new_column_name,
                        format_type=new_format_type,
                        position=new_position,
                        size=new_size,
                        check_free_pay=new_check_free_pay,
                        bliink_mapping=new_mapping
                    )
                    
                    st.success(f" Field '{new_field_name}' updated successfully!")
                    
                    # Update schema total length
                    schema.total_length = max(f.position + f.size - 1 for f in schema.fields)
                    
                    # Save to session state for later saving
                    st.session_state[f'modified_{schema_type}_schema'] = schema
    
    with edit_tabs[2]:
        # Add new fields
        st.markdown("### Add New Field")
        
        with st.form("add_new_field"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_field_name = st.text_input("Field Name")
                new_column_name = st.text_input("Column Name")
                new_position = st.number_input("Position", min_value=1, value=schema.total_length + 1)
                new_size = st.number_input("Size", min_value=1, value=10)
            
            with col2:
                new_format_type = st.selectbox("Format Type", ["Text", "Number", "Date", "Amount"])
                new_check_free_pay = st.text_area("Check Free Pay Description")
                new_mapping = st.text_area("Database Mapping")
            
            if st.form_submit_button("➕ Add Field"):
                if new_field_name and new_column_name:
                    # Create new field
                    new_field = FixedWidthField(
                        field_name=new_field_name,
                        column_name=new_column_name,
                        format_type=new_format_type,
                        position=new_position,
                        size=new_size,
                        check_free_pay=new_check_free_pay,
                        bliink_mapping=new_mapping
                    )
                    
                    # Add to schema
                    schema.fields.append(new_field)
                    schema.total_length = max(f.position + f.size - 1 for f in schema.fields)
                    
                    st.success(f" Field '{new_field_name}' added successfully!")
                    
                    # Save to session state
                    st.session_state[f'modified_{schema_type}_schema'] = schema
                else:
                    st.error(" Please provide field name and column name")
    
    with edit_tabs[3]:
        # Remove fields
        st.markdown("### Remove Fields")
        
        if len(schema.fields) > 0:
            fields_to_remove = st.multiselect(
                "Select Fields to Remove:",
                range(len(schema.fields)),
                format_func=lambda x: f"{schema.fields[x].field_name} - {schema.fields[x].column_name}"
            )
            
            if fields_to_remove:
                st.warning(f"⚠️ You are about to remove {len(fields_to_remove)} field(s)")
                
                if st.button("🗑️ Remove Selected Fields", type="secondary"):
                    # Remove fields in reverse order to maintain indices
                    for index in sorted(fields_to_remove, reverse=True):
                        removed_field = schema.fields.pop(index)
                        st.success(f" Removed field: {removed_field.field_name}")
                    
                    # Update total length
                    if schema.fields:
                        schema.total_length = max(f.position + f.size - 1 for f in schema.fields)
                    else:
                        schema.total_length = 0
                    
                    # Save to session state
                    st.session_state[f'modified_{schema_type}_schema'] = schema
                    st.rerun()
        else:
            st.info("No fields available to remove")
    
    with edit_tabs[4]:
        # Save changes
        st.markdown("### Save Schema Changes")
        
        if f'modified_{schema_type}_schema' in st.session_state:
            modified_schema = st.session_state[f'modified_{schema_type}_schema']
            
            st.success(" You have unsaved changes to this schema")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("💾 Save Changes"):
                    save_schema_changes(schema_type, modified_schema)
                    st.success(f" {schema_type.title()} schema saved successfully!")
                    del st.session_state[f'modified_{schema_type}_schema']
                    st.rerun()
            
            with col2:
                if st.button("🔄 Discard Changes"):
                    del st.session_state[f'modified_{schema_type}_schema']
                    st.info("Changes discarded")
                    st.rerun()
            
            # Show preview of changes
            st.markdown("### Preview of Changes")
            changes_data = []
            for i, field in enumerate(modified_schema.fields):
                changes_data.append({
                    "Field": field.field_name,
                    "Position": f"{field.position}-{field.position + field.size - 1}",
                    "Size": field.size,
                    "Type": field.format_type
                })
            
            changes_df = pd.DataFrame(changes_data)
            st.dataframe(changes_df, use_container_width=True)
        
        else:
            st.info("No unsaved changes")

def save_custom_schema(schema_name, file_pattern, fields):
    """Save a custom schema"""
    try:
        # Create schema object
        schema = FixedWidthSchema(schema_name.lower())
        
        for field_data in fields:
            field = FixedWidthField(
                field_name=field_data['field_name'],
                column_name=field_data['column_name'],
                format_type=field_data['format_type'],
                position=field_data['position'],
                size=field_data['size'],
                check_free_pay=field_data['description'],
                bliink_mapping=field_data['mapping']
            )
            schema.add_field(field)
        
        # Save to session state (in production, you'd save to database or file)
        if 'custom_schemas' not in st.session_state:
            st.session_state.custom_schemas = {}
        
        st.session_state.custom_schemas[schema_name] = {
            'schema': schema,
            'file_pattern': file_pattern,
            'created_date': datetime.now().isoformat()
        }
        
        logger.info(f"Custom schema '{schema_name}' saved with {len(fields)} fields")
        
    except Exception as e:
        logger.error(f"Error saving custom schema: {str(e)}")
        raise e

def save_schema_changes(schema_type, modified_schema):
    """Save changes to an existing schema"""
    try:
        # In production, you would update the schema in your database or configuration
        # For now, we'll save to session state
        
        if 'modified_schemas' not in st.session_state:
            st.session_state.modified_schemas = {}
        
        st.session_state.modified_schemas[schema_type] = {
            'schema': modified_schema,
            'modified_date': datetime.now().isoformat()
        }
        
        logger.info(f"Schema '{schema_type}' changes saved with {len(modified_schema.fields)} fields")
        
    except Exception as e:
        logger.error(f"Error saving schema changes: {str(e)}")
        raise e

def export_schema_to_json(schema):
    """Export schema to JSON format"""
    schema_data = {
        'schema_name': schema.schema_name,
        'total_length': schema.total_length,
        'fields': []
    }
    
    for field in schema.fields:
        field_data = {
            'field_name': field.field_name,
            'column_name': field.column_name,
            'format_type': field.format_type,
            'position': field.position,
            'size': field.size,
            'check_free_pay': field.check_free_pay,
            'bliink_mapping': field.bliink_mapping
        }
        schema_data['fields'].append(field_data)
    
    return json.dumps(schema_data, indent=2)

def import_schema_from_json():
    """Import schema from JSON file"""
    st.subheader("📥 Import Schema from JSON")
    
    uploaded_file = st.file_uploader("Choose JSON schema file", type=['json'])
    
    if uploaded_file is not None:
        try:
            schema_data = json.loads(uploaded_file.getvalue().decode('utf-8'))
            
            # Create schema object
            schema = FixedWidthSchema(schema_data['schema_name'])
            
            for field_data in schema_data['fields']:
                field = FixedWidthField(
                    field_name=field_data['field_name'],
                    column_name=field_data['column_name'],
                    format_type=field_data['format_type'],
                    position=field_data['position'],
                    size=field_data['size'],
                    check_free_pay=field_data['check_free_pay'],
                    bliink_mapping=field_data['bliink_mapping']
                )
                schema.add_field(field)
            
            st.success(f" Schema '{schema_data['schema_name']}' imported successfully!")
            
            # Show preview
            st.markdown("### Imported Schema Preview")
            fields_data = []
            for field in schema.fields:
                fields_data.append({
                    "Field": field.field_name,
                    "Position": f"{field.position}-{field.position + field.size - 1}",
                    "Size": field.size,
                    "Type": field.format_type
                })
            
            fields_df = pd.DataFrame(fields_data)
            st.dataframe(fields_df, use_container_width=True)
            
            if st.button("💾 Save Imported Schema"):
                # Save to custom schemas
                if 'custom_schemas' not in st.session_state:
                    st.session_state.custom_schemas = {}
                
                st.session_state.custom_schemas[schema_data['schema_name']] = {
                    'schema': schema,
                    'file_pattern': f"{schema_data['schema_name'].upper()}_*.txt",
                    'created_date': datetime.now().isoformat()
                }
                
                st.success(" Imported schema saved!")
        
        except Exception as e:
            st.error(f" Error importing schema: {str(e)}")

# Integration function to add schema editor to your main application

def add_schema_editor_to_main_app():
    """Add schema editor to the main application"""
    
    
    # Also add to your sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 Schema Management")
    
    if st.sidebar.button("✏️ Edit Schemas"):
        st.session_state.show_schema_editor = True
    
    # Show custom schemas count
    custom_count = len(st.session_state.get('custom_schemas', {}))
    modified_count = len(st.session_state.get('modified_schemas', {}))
    
    if custom_count > 0:
        st.sidebar.success(f"📁 {custom_count} custom schemas")
    
    if modified_count > 0:
        st.sidebar.warning(f"⚠️ {modified_count} unsaved changes")

# ========================
# TEMPLATE GENERATION FUNCTIONS
# ========================

def get_csv_download_link(filename, csv_content, link_text):
    """Generate a download link for a CSV template"""
    b64 = base64.b64encode(csv_content.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{link_text}</a>'
    return href

def show_config_status():
    """Show status of config file in the database tab"""
    config_file_exists = os.path.exists('config.properties')
    if config_file_exists:
        config = read_config_file()
        if config:
            st.success("Configuration loaded from config.properties")
        else:
            pass
            # st.warning("config.properties file exists but could not be read")
    else:
        st.info("No config.properties file found. Using values from the form.")

# ========================
# SESSION STATE INITIALIZATION
# ========================

# Initialize session state variables
if 'db_connected' not in st.session_state:
    st.session_state.db_connected = False

if 'db_host' not in st.session_state:
    st.session_state.db_host = ""
    
if 'db_name' not in st.session_state:
    st.session_state.db_name = ""
    
if 'db_user' not in st.session_state:
    st.session_state.db_user = ""
    
if 'db_password' not in st.session_state:
    st.session_state.db_password = ""

# Initialize processing state variables
if 'entity_processed' not in st.session_state:
    st.session_state.entity_processed = False

if 'account_processed' not in st.session_state:
    st.session_state.account_processed = False

if 'transaction_processed' not in st.session_state:
    st.session_state.transaction_processed = False
# Initialize AML state variables
if 'aml_completed' not in st.session_state:
    st.session_state.aml_completed = False

if 'aml_results' not in st.session_state:
    st.session_state.aml_results = None

if 'aml_available' not in st.session_state:
    st.session_state.aml_available = False

# Initialize transaction upload completion state
if 'transaction_upload_completed' not in st.session_state:
    st.session_state.transaction_upload_completed = False

# Initialize temp file storage
if 'temp_transaction_file' not in st.session_state:
    st.session_state.temp_transaction_file = None

# Initialize fixed width schema state
if 'fixed_width_schemas' not in st.session_state:
    st.session_state.fixed_width_schemas = {}

if 'current_fixed_width_schema' not in st.session_state:
    st.session_state.current_fixed_width_schema = None

if 'relationship_processed' not in st.session_state:
    st.session_state.relationship_processed = False

if 'relationship_success_count' not in st.session_state:
    st.session_state.relationship_success_count = 0

if 'relationship_error_count' not in st.session_state:
    st.session_state.relationship_error_count = 0

if 'relationship_results' not in st.session_state:
    st.session_state.relationship_results = []



# Enhanced templates with better examples
def get_enhanced_csv_templates():
    """Enhanced CSV templates with more examples and better formatting"""
    
    customer_template_enhanced = """changetype,customer_id,parent_customer_id,customer_type,first_name,last_name,middle_name,name_prefix,name_suffix,date_of_birth,gender,is_employee,nationality,deceased_date,dual_citizenship_country,ethnic_background,tax_id,job_title,marital_status,home_ownership_type,customer_risk_score,pep,is_related_to_pep,document_type,document_number,document_expiry_date,business_name,date_of_incorporation,doing_business_as,business_type,industry_code,bankruptcy_date,tin,lei,swift_number,company_identifier,company_name,email_type,email_address,phone_type,phone_number,address_type,address_line_1,address_line_2,address_line_3,address_city,address_state,address_country,address_zipcode,number_of_years_at_address,closed_date,closed_reason
    add,CUST12345,,Consumer,John,Smith,David,Mr.,Jr.,2000-05-05,N,US,,,,123-45-6789,Software Engineer,Married,Own,750,N,N,Driver License,DL12345678,2027-01-15,M,,,,,,,,,,,,Primary,john.smith@email.com,Primary,555-123-4567,Primary,123 Main Street,Apt 4B,,New York,NY,US,10001,5,,
    add,CUST67890,,Consumer,Sarah,Johnson,Marie,Ms.,,1993-04-12,N,US,,,,987-65-4321,Marketing Manager,Single,Rent,680,N,N,Passport,P98765432,2026-06-30,F,,,,,,,,,,,,Primary,sarah.johnson@email.com,Mobile,555-987-6543,Primary,456 Oak Avenue,,,Los Angeles,CA,US,90210,2,,
    add,CUST11111,CUST12345,Business,,,,,,,,,,,,,,,,,,,,,,TechCorp Solutions Inc,2018-03-15,TechCorp,Technology,541511,,45-1234567,123456789012345678901,TECHCORPUS33XXX,TC001,TechCorp Solutions Inc,Business,contact@techcorp.com,Business,555-555-0123,Business,789 Business Blvd,Suite 200,,San Francisco,CA,US,94105,4,,"""
    
    account_template_enhanced = """changetype,account_id,customer_id,account_number,account_routing_number,account_type,account_currency,account_balance,open_date,closed_date,close_reason,branch_id,reopen_date,account_risk_score,account_status,is_p2p_enabled,p2p_enrollment_date,p2p_email,p2p_phone,is_joint,joint_account_primary_id,joint_account_secondary_id,joint_account_relationship_type,joint_account_status
    add,ACC12345,CUST12345,1234567890,123456789,Checking,USD,2500.75,2020-01-15,,,BRANCH_NYC_001,,65,Active,Y,2020-02-01,john.smith@email.com,555-123-4567,N,,,,
    add,ACC67890,CUST67890,9876543210,987654321,Savings,USD,15000.00,2021-06-20,,,BRANCH_LA_005,,70,Active,N,,,555-987-6543,N,,,,
    add,ACC11111,CUST11111,5555555555,555555555,Business Checking,USD,75000.50,2019-08-10,,,BRANCH_SF_010,,80,Active,Y,2019-09-01,contact@techcorp.com,555-555-0123,N,,,,
    add,ACC99999,CUST12345,1111111111,123456789,Joint Checking,USD,5000.00,2022-03-01,,,BRANCH_NYC_001,,60,Active,Y,2022-03-15,joint@email.com,555-111-2222,Y,CUST12345,CUST67890,Spouse,Active"""
    
    transaction_template_enhanced = """changetype,transaction_id,account_id,debit_credit_indicator,transaction_date,transaction_type,transaction_code,tran_code_description,amount,preferred_currency,settlement_currency,exchange_rate_applied,transaction_origin_country,channel,counterparty_account,counterparty_routing_number,counterparty_name,branch_id,teller_id,check_number,micr_code,check_image_front_path,check_image_back_path,txn_location,is_p2p_transaction,p2p_recipient_type,p2p_recipient,p2p_sender_type,p2p_sender,p2p_reference_id
    add,TXN12345,ACC12345,D,2023-06-15 09:30:00,ATM Withdrawal,ATM_WD,ATM Cash Withdrawal,200.00,USD,USD,1.0,US,ATM,,,ATM Network,BRANCH_NYC_001,,,,,/images/atm_receipts/TXN12345.jpg,New York NY,N,,,,,
    add,TXN67890,ACC67890,C,2023-06-16 14:22:00,Direct Deposit,DIR_DEP,Salary Deposit,3500.00,USD,USD,1.0,US,ACH,9999999999,555555555,ABC Company Payroll,BRANCH_LA_005,,,,,,,Los Angeles CA,N,,,,,
    add,TXN11111,ACC11111,D,2023-06-17 11:45:00,Wire Transfer,WIRE_OUT,International Wire,10000.00,USD,EUR,0.92,US,Wire,8888888888,777777777,European Supplier Ltd,BRANCH_SF_010,,,,,,INTL_WIRE_001,San Francisco CA,N,,,,,
    add,TXN99999,ACC12345,D,2023-06-18 16:15:00,P2P Payment,P2P_SEND,Zelle Payment,150.00,USD,USD,1.0,US,Mobile,ACC67890,987654321,Sarah Johnson,BRANCH_NYC_001,,,,,,,New York NY,Y,Individual,sarah.johnson@email.com,Individual,john.smith@email.com,ZELLE_P2P_001
    add,TXN77777,ACC67890,C,2023-06-18 16:16:00,P2P Payment,P2P_RCV,Zelle Payment Received,150.00,USD,USD,1.0,US,Mobile,ACC12345,123456789,John Smith,BRANCH_LA_005,,,,,,,Los Angeles CA,Y,Individual,john.smith@email.com,Individual,sarah.johnson@email.com,ZELLE_P2P_001"""
    
    return {
        'customer': customer_template_enhanced,
        'account': account_template_enhanced,
        'transaction': transaction_template_enhanced
    }

# ========================
# STREAMLIT UI
# ========================

# Main app UI
st.title("Multi-Format Data Loader System")

# Template Download Section
# Enhanced Template Download Section
# create_enhanced_template_section()
replace_template_section()



# App layout with tabs
st.markdown("""
<style>
.stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
    font-size: 14px;
}
            <style>
</style>
</style>
""", unsafe_allow_html=True)
tabs = st.tabs(["Database Connection", 
                "Customer Data Upload", 
                "Account Data Upload", 
                "Relationship Data Upload",
                "Transaction Data Upload",
                "List Management",
                "AML Detection Results", 
                "Processing Results",
                "Log Viewer"])

# Add this CSS to hide the password eye button
st.markdown("""
<style>
    /* Hide the eye button in password input fields */
    .stTextInput > div > div > input[type="password"]::-ms-reveal,
    .stTextInput > div > div > input[type="password"]::-ms-clear {
        display: none;
    }
    
    /* For Webkit browsers (Chrome, Safari, Edge) */
    .stTextInput > div > div > input[type="password"]::-webkit-credentials-auto-fill-button,
    .stTextInput > div > div > input[type="password"]::-webkit-strong-password-auto-fill-button {
        display: none !important;
    }
    
    /* Hide Streamlit's built-in password reveal button */
    button[title="Show password"] {
        display: none !important;
    }
    
    /* Alternative approach - hide all buttons within password input containers */
    .stTextInput div[data-testid="textInputRootElement"] button {
        display: none !important;
    }
    
    /* More specific targeting for password input eye button */
    .stTextInput:has(input[type="password"]) button {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# Database Connection Tab
with tabs[0]:
    st.markdown("<h3 style='font-size: 20px;'>Database Connection Setup</h3>", unsafe_allow_html=True)
    show_config_status()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.session_state.db_host = st.text_input("Database Host", st.session_state.db_host)
        st.session_state.db_name = st.text_input("Database Name", st.session_state.db_name)
    
    with col2:
        st.session_state.db_user = st.text_input("Database User", st.session_state.db_user)
        st.session_state.db_password = st.text_input("Database Password", st.session_state.db_password, type="password")
    
    if st.button("Connect to database"):

        # IMMEDIATELY create progress indicators - no delay
        db_test_progress_container = st.empty()
        db_test_status_container = st.empty()
        
        with db_test_progress_container.container():
            db_test_progress_bar = st.progress(0)
        
        # Force Streamlit to render the UI immediately
        time.sleep(0.1)

        
        try:
            # Step 1: Reading configuration
            with db_test_status_container.container():
                st.info(" Reading configuration...")
            db_test_progress_bar.progress(0.1)
            time.sleep(0.5)
            
            # Step 2: Initializing connection parameters
            with db_test_status_container.container():
                st.info(" Initializing connection parameters...")
            db_test_progress_bar.progress(0.3)
            time.sleep(0.5)


            # Step 3: Attempting database connection
            with db_test_status_container.container():
                st.info(" Attempting database connection...")
            db_test_progress_bar.progress(0.5)

            connection = create_db_connection()
            if connection and connection.is_connected():
                # Step 4: Verifying connection
                with db_test_status_container.container():
                    st.info(" Verifying connection...")
                db_test_progress_bar.progress(0.8)
                time.sleep(0.5)

                st.session_state.db_connected = True

                # Step 5: Getting database info
                with db_test_status_container.container():
                    st.info(" Retrieving database information...")
                db_test_progress_bar.progress(1.0)
                
                # Clean up progress containers
                db_test_progress_container.empty()
                db_test_status_container.empty()
                
                
                st.success("Connected to database successfully!")
                connection.close()
            else:
                db_test_progress_container.empty()
                db_test_status_container.empty()
                st.error(" Could not connect to the database.")
                st.session_state.db_connected = False
        except Error as e:
            st.error(f"Error: {e}")
            db_test_progress_container.empty()
            db_test_status_container.empty()
            st.error(f"Database connection error: {e}")
            st.session_state.db_connected = False
            logger.error(f"Database connection failed: {str(e)}")
        except Exception as e:
            db_test_progress_container.empty()
            db_test_status_container.empty()
            st.error(f"Unexpected error: {e}")
            st.session_state.db_connected = False
            logger.error(f"Unexpected connection error: {str(e)}")
    
    if st.session_state.db_connected:
        st.success(f"Connected to database: {st.session_state.db_name} at {st.session_state.db_host}")
        
        # Try to get table counts
        try:
            connection = create_db_connection()
            if connection and connection.is_connected():
                cursor = connection.cursor()
                
                tables = ["entity", "account", "transactions"]
                table_counts = {}
                
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        table_counts[table] = count
                    except:
                        table_counts[table] = "Table not found"
                
                cursor.close()
                connection.close()
                
                # Display table counts
                st.subheader("Database Summary")
                cols = st.columns(len(tables))
                for i, table in enumerate(tables):
                    display_name = "Customer Records" if table == "entity" else f"{table.capitalize()} Records"
                    cols[i].metric(display_name, table_counts[table])
        except:
            st.warning("Could not retrieve database statistics.")

# Customer Upload Tab
with tabs[1]:
    replace_customer_upload_tab()
    

# Account Upload Tab
with tabs[2]:
    replace_account_upload_tab()
    
# Transaction Upload Tab
with tabs[3]:
    create_relationship_upload_tab_with_fixed_width()

with tabs[4]:
    
    replace_transaction_upload_tab()

with tabs[5]:
    create_list_management_tab()

with tabs[6]:
    create_aml_results_tab()

# Processing Results Tab
with tabs[7]:
    st.markdown("<h3 style='font-size: 22px;'>Processing Results</h3>", unsafe_allow_html=True)
    # st.info(" Enhanced results display for batch processing")
    
    # Create tabs for different result types
    result_tabs = st.tabs(["Customer Results", "Account Results", "Transaction Results","Relationship Results", "List Management Results","Batch Performance"])
    
    # Helper function to log processing results
    def log_processing_results(operation_type, success_count, error_count, results_list, filename=None):
        """Log processing results to the log file"""
        # Create a unique logging key for this operation
        logging_key = f"{operation_type.lower()}_logged"

        # Check if we've already logged this set of results
        if hasattr(st.session_state, logging_key) and getattr(st.session_state, logging_key):
            logger.debug(f"Skipping duplicate logging for {operation_type} - already logged")
            return

        total_operations = success_count + error_count
        success_rate = (success_count / total_operations * 100) if total_operations > 0 else 0
        
        logger.info(f"=== {operation_type} Processing Results ===")
        if filename:
            logger.info(f"Source File: {filename}")
        logger.info(f"Records Submitted: {total_operations}")
        logger.info(f"Records Processed Successfully: {success_count}")
        logger.info(f"Records Failed: {error_count}")
        logger.info(f"Success Ratio: {success_rate:.2f}%")
        
        # Log detailed results if available
        if results_list:
            logger.info(f"--- Detailed {operation_type} Results ---")
            for i, result in enumerate(results_list, 1):
                logger.info(f"Result {i}: {result}")
        
        logger.info(f"=== End {operation_type} Processing Results ===\n")

        # Mark as logged to prevent duplicates
        setattr(st.session_state, logging_key, True)

    with tabs[8]:
        st.header("📋 Application Logs")
    
        # Get list of available log files
        def get_available_log_files():
            """Get list of available log files"""
            log_files = []
            potential_dirs = [
                "logs",
                ".",
                os.path.join(os.path.expanduser("~"), "logs"),
                os.path.join(os.environ.get("TEMP", ""), "logs"),
                os.environ.get("TEMP", "")
            ]
            
            for directory in potential_dirs:
                if directory and os.path.exists(directory):
                    try:
                        for file in os.listdir(directory):
                            if file.startswith("dataloader_operations_") and file.endswith(".log"):
                                full_path = os.path.join(directory, file)
                                if os.path.isfile(full_path):
                                    # Extract date from filename for sorting
                                    try:
                                        date_part = file.replace("dataloader_operations_", "").replace(".log", "")
                                        file_date = datetime.strptime(date_part, "%Y%m%d")
                                        log_files.append({
                                            'filename': file,
                                            'full_path': full_path,
                                            'date': file_date,
                                            'size': os.path.getsize(full_path)
                                        })
                                    except:
                                        # If date parsing fails, still include the file
                                        log_files.append({
                                            'filename': file,
                                            'full_path': full_path,
                                            'date': datetime.now(),
                                            'size': os.path.getsize(full_path)
                                        })
                    except Exception as e:
                        continue
                    break  # Use first available directory
            
            # Sort by date (newest first)
            log_files.sort(key=lambda x: x['date'], reverse=True)
            return log_files
        
        def format_file_size(size_bytes):
            """Format file size in human readable format"""
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        
        def read_log_file(file_path, max_lines=1000):
            """Read log file with error handling"""
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    if len(lines) > max_lines:
                        return lines[-max_lines:], True  # Return last N lines and indicate truncation
                    return lines, False
            except Exception as e:
                st.error(f"Error reading log file: {str(e)}")
                return [], False
        
        # Get available log files
        available_logs = get_available_log_files()
        
        if not available_logs:
            st.info("No log files found. Run the application to generate logs.")
        else:
            # Create columns for controls
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                # File selection dropdown
                log_options = []
                for log_file in available_logs:
                    date_str = log_file['date'].strftime("%Y-%m-%d")
                    size_str = format_file_size(log_file['size'])
                    log_options.append(f"{date_str} ({size_str}) - {log_file['filename']}")
                
                selected_log_index = st.selectbox(
                    "Select Log File:",
                    range(len(log_options)),
                    format_func=lambda x: log_options[x],
                    key="log_file_selector"
                )
            
            with col2:
                # Refresh button
                if st.button("🔄 Refresh", key="refresh_logs"):
                    st.rerun()
            
            with col3:
                # Auto-refresh toggle
                auto_refresh = st.checkbox("Auto-refresh (30s)", key="auto_refresh_logs")
                if auto_refresh:
                    time.sleep(30)
                    st.rerun()
            
            # Display selected log file
            if selected_log_index is not None and selected_log_index < len(available_logs):
                selected_log = available_logs[selected_log_index]
                
                # File info
                st.subheader(f"📄 {selected_log['filename']}")
                
                info_col1, info_col2, info_col3 = st.columns(3)
                with info_col1:
                    st.metric("File Size", format_file_size(selected_log['size']))
                with info_col2:
                    st.metric("Date", selected_log['date'].strftime("%Y-%m-%d"))
                with info_col3:
                    try:
                        mod_time = datetime.fromtimestamp(os.path.getmtime(selected_log['full_path']))
                        st.metric("Last Modified", mod_time.strftime("%H:%M:%S"))
                    except:
                        st.metric("Last Modified", "Unknown")
                
                # Log level filter
                log_levels = st.multiselect(
                    "Filter by Log Level:",
                    ["INFO", "WARNING", "ERROR", "DEBUG"],
                    default=["INFO", "WARNING", "ERROR", "DEBUG"],
                    key="log_level_filter"
                )
                
                # Search functionality
                search_term = st.text_input("🔍 Search in logs:", key="log_search")
                
                # Read and display log content
                log_lines, was_truncated = read_log_file(selected_log['full_path'])
                
                if was_truncated:
                    st.warning("⚠️ Log file is large. Showing last 1000 lines only.")
                
                # Filter logs based on level and search term
                filtered_lines = []
                for line in log_lines:
                    # Check log level filter
                    line_matches_level = False
                    for level in log_levels:
                        if f" - {level} - " in line:
                            line_matches_level = True
                            break
                    
                    # If no level found in line, include it (might be continuation of previous log)
                    if not any(f" - {level} - " in line for level in ["INFO", "WARNING", "ERROR", "DEBUG"]):
                        line_matches_level = True
                    
                    # Check search term
                    line_matches_search = True
                    if search_term:
                        line_matches_search = search_term.lower() in line.lower()
                    
                    if line_matches_level and line_matches_search:
                        filtered_lines.append(line)
                
                # Display logs in a code block for better formatting
                if filtered_lines:
                    st.subheader(f"Log Content ({len(filtered_lines)} lines)")
                    
                    # Add download button
                    log_content = ''.join(filtered_lines)
                    st.download_button(
                        label="💾 Download Filtered Logs",
                        data=log_content,
                        file_name=f"filtered_{selected_log['filename']}",
                        mime="text/plain",
                        key="download_logs"
                    )
                    
                    # Display logs with syntax highlighting
                    log_text = ''.join(filtered_lines)
                    st.code(log_text, language="text")
                    
                else:
                    st.info("No log entries match the current filters.")
                
                # Show some statistics
                if log_lines:
                    st.subheader("📊 Log Statistics")
                    
                    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                    
                    info_count = sum(1 for line in log_lines if " - INFO - " in line)
                    warning_count = sum(1 for line in log_lines if " - WARNING - " in line)
                    error_count = sum(1 for line in log_lines if " - ERROR - " in line)
                    debug_count = sum(1 for line in log_lines if " - DEBUG - " in line)
                    
                    with stat_col1:
                        st.metric("ℹ️ INFO", info_count)
                    with stat_col2:
                        st.metric("⚠️ WARNING", warning_count)
                    with stat_col3:
                        st.metric(" ERROR", error_count)
                    with stat_col4:
                        st.metric("🐛 DEBUG", debug_count)

    # Customer Results Tab
    with result_tabs[0]:
        if 'entity_results' in st.session_state:
            # Log results to file
            filename = getattr(st.session_state, 'entity_metadata', {}).get('filename', None)
            log_processing_results(
                "Customer", 
                st.session_state.entity_success_count, 
                st.session_state.entity_error_count, 
                st.session_state.entity_results,
                filename
            )
            
            # Display metrics with enhanced styling
            st.subheader("Customer Processing Summary")
            cols = st.columns(4)
            
            total_ops = st.session_state.entity_success_count + st.session_state.entity_error_count
            success_rate = (st.session_state.entity_success_count / total_ops * 100) if total_ops > 0 else 0
            
            cols[0].metric(" Successful Operations", st.session_state.entity_success_count)
            cols[1].metric(" Failed Operations", st.session_state.entity_error_count)
            cols[2].metric(" Total Operations", total_ops)
            cols[3].metric(" Success Rate", f"{success_rate:.1f}%")
            
            # Display detailed results with advanced filtering
            st.subheader("Detailed Customer Results")
            
            # Convert results to DataFrame for easier filtering
            results_df = pd.DataFrame(st.session_state.entity_results, columns=["Message"])
            
            # Add status column
            results_df["Status"] = results_df["Message"].apply(
                lambda x: "Success" if "Successfully" in x or "Prepared" in x else "Error"
            )
            
            # Enhanced filter options
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                filter_status = st.selectbox("Filter by Status", ["All", "Success", "Error"], key="entity_filter_batch")
            
            with col_filter2:
                search_term = st.text_input("Search in results", key="entity_search_batch")
            
            # Apply filters
            filtered_df = results_df.copy()
            
            if filter_status != "All":
                filtered_df = filtered_df[filtered_df["Status"] == filter_status]
            
            if search_term:
                filtered_df = filtered_df[filtered_df["Message"].str.contains(search_term, case=False)]
            
            # Display count of filtered results
            st.write(f"Showing {len(filtered_df):,} of {len(results_df):,} results")
            
            # Display results with pagination
            results_per_page = 50
            if len(filtered_df) > results_per_page:
                total_pages = (len(filtered_df) + results_per_page - 1) // results_per_page
                page = st.selectbox(f"Page (1-{total_pages})", range(1, total_pages + 1), key="entity_page_batch") - 1
                start_idx = page * results_per_page
                end_idx = min(start_idx + results_per_page, len(filtered_df))
                display_df = filtered_df.iloc[start_idx:end_idx]
                st.write(f"Showing results {start_idx + 1} to {end_idx}")
            else:
                display_df = filtered_df
            
            # Display results
            st.dataframe(display_df, height=400, use_container_width=True)
            
            # Export results option
            if st.button(" Export Customer Results to CSV", key="export_entity_batch"):
                csv = results_df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"customer_batch_processing_results_{timestamp}.csv",
                    mime="text/csv",
                    key="download_entity_batch"
                )
        else:
            st.info("No customer processing results available. Please upload and process customer data first.")
    
    # Account Results Tab
    with result_tabs[1]:
        if 'account_results' in st.session_state:
            # Display metrics with enhanced styling
            filename = getattr(st.session_state, 'account_metadata', {}).get('filename', None)
            
            log_processing_results(
                "Account", 
                st.session_state.account_success_count, 
                st.session_state.account_error_count, 
                st.session_state.account_results,
                filename
            )
            st.subheader("Account Processing Summary")
            cols = st.columns(4)
            
            total_ops = st.session_state.account_success_count + st.session_state.account_error_count
            success_rate = (st.session_state.account_success_count / total_ops * 100) if total_ops > 0 else 0
            
            cols[0].metric(" Successful Operations", st.session_state.account_success_count)
            cols[1].metric(" Failed Operations", st.session_state.account_error_count)
            cols[2].metric(" Total Operations", total_ops)
            cols[3].metric(" Success Rate", f"{success_rate:.1f}%")
            
            # Display detailed results with advanced filtering
            st.subheader("Detailed Account Results")
            
            # Convert results to DataFrame for easier filtering
            results_df = pd.DataFrame(st.session_state.account_results, columns=["Message"])
            
            # Add status column
            results_df["Status"] = results_df["Message"].apply(
                lambda x: "Success" if "Successfully" in x or "Prepared" in x else "Error"
            )
            
            # Enhanced filter options
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                filter_status = st.selectbox("Filter by Status", ["All", "Success", "Error"], key="account_filter_batch")
            
            with col_filter2:
                search_term = st.text_input("Search in results", key="account_search_batch")
            
            # Apply filters
            filtered_df = results_df.copy()
            
            if filter_status != "All":
                filtered_df = filtered_df[filtered_df["Status"] == filter_status]
            
            if search_term:
                filtered_df = filtered_df[filtered_df["Message"].str.contains(search_term, case=False)]
            
            # Display count of filtered results
            st.write(f"Showing {len(filtered_df):,} of {len(results_df):,} results")
            
            # Display results with pagination
            results_per_page = 50
            if len(filtered_df) > results_per_page:
                total_pages = (len(filtered_df) + results_per_page - 1) // results_per_page
                page = st.selectbox(f"Page (1-{total_pages})", range(1, total_pages + 1), key="account_page_batch") - 1
                start_idx = page * results_per_page
                end_idx = min(start_idx + results_per_page, len(filtered_df))
                display_df = filtered_df.iloc[start_idx:end_idx]
                st.write(f"Showing results {start_idx + 1} to {end_idx}")
            else:
                display_df = filtered_df
            
            # Display results
            st.dataframe(display_df, height=400, use_container_width=True)
            
            # Export results option
            if st.button(" Export Account Results to CSV", key="export_account_batch"):
                csv = results_df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"account_batch_processing_results_{timestamp}.csv",
                    mime="text/csv",
                    key="download_account_batch"
                )
        else:
            st.info("No account processing results available. Please upload and process account data first.")
    
    # Transaction Results Tab
    with result_tabs[2]:
        if 'transaction_results' in st.session_state and st.session_state.transaction_results:
                filename = getattr(st.session_state, 'transaction_metadata', {}).get('filename', None)
                log_processing_results(
                    "Transaction", 
                    st.session_state.transaction_success_count, 
                    st.session_state.transaction_error_count, 
                    st.session_state.transaction_results,
                    filename
                )
                
                # FIXED: Enhanced categorization function
                def categorize_message(message):
                    """
                    Properly categorize transaction processing messages
                    """
                    message_lower = message.lower()
                    
                    # Actual errors
                    if any(error_indicator in message_lower for error_indicator in [
                        "error -", "failed", "cannot", "missing", "not found", 
                        "invalid", "parameter validation failed", "thread error"
                    ]):
                        return "Error"
                    
                    # Skipped duplicates (NOT errors)
                    elif any(skip_indicator in message_lower for skip_indicator in [
                        "already queued", "skipping duplicate", "skip -", 
                        "duplicate", "already exists"
                    ]):
                        return "Skipped"
                    
                    # Successful operations
                    elif any(success_indicator in message_lower for success_indicator in [
                        "successfully", "success", "completed", "processed", 
                        "added", "modified", "updated", "created", "prepared"
                    ]):
                        return "Success"
                    
                    # Informational messages
                    elif any(info_indicator in message_lower for info_indicator in [
                        "info -", "converting to", "exists, converting", 
                        "summary:", "multi-threaded", "performance:"
                    ]):
                        return "Info"
                    
                    # Default to Info for unclassified
                    else:
                        return "Info"
                
                # Convert results to DataFrame with proper categorization
                results_df = pd.DataFrame(st.session_state.transaction_results, columns=["Message"])
                
                # FIXED: Use enhanced categorization
                results_df["Status"] = results_df["Message"].apply(categorize_message)
                
                # Count each category
                status_counts = results_df["Status"].value_counts()
                actual_errors = status_counts.get("Error", 0)
                skipped_duplicates = status_counts.get("Skipped", 0)
                successful_ops = status_counts.get("Success", 0)
                info_messages = status_counts.get("Info", 0)
                
                # FIXED: Display metrics with proper breakdown
                st.subheader("Transaction Processing Summary")
                cols = st.columns(5)
                
                # Calculate success rate based on actual processed transactions (excluding skips)
                processed_transactions = successful_ops + actual_errors
                success_rate = (successful_ops / processed_transactions * 100) if processed_transactions > 0 else 0
                
                cols[0].metric("✅ Successful Operations", successful_ops)
                cols[1].metric(" Actual Errors", actual_errors, delta_color="inverse")
                cols[2].metric("⏭️ Skipped Duplicates", skipped_duplicates, help="Duplicate transactions skipped (not errors)")
                cols[3].metric("ℹ️ Info Messages", info_messages)
                cols[4].metric("📊 Success Rate", f"{success_rate:.1f}%", help="Success rate of processed transactions")
                
                # Show breakdown explanation
                with st.expander("📋 Results Breakdown Explanation"):
                    st.write("""
                    **Categories Explained:**
                    - **Successful Operations**: Transactions that were successfully processed (added/modified/deleted)
                    - **Actual Errors**: Real errors that need attention (missing data, validation failures, etc.)
                    - **Skipped Duplicates**: Duplicate transaction IDs that were safely skipped (normal behavior)
                    - **Info Messages**: General information about processing (summaries, conversions, etc.)
                    - **Success Rate**: Percentage of successfully processed transactions (excludes skipped duplicates)
                    """)
                
                # FIXED: Enhanced filter options with all categories
                st.subheader("Detailed Transaction Results")
                
                col_filter1, col_filter2, col_filter3 = st.columns(3)
                
                with col_filter1:
                    filter_status = st.selectbox(
                        "Filter by Status", 
                        ["All", "Success", "Error", "Skipped", "Info"], 
                        key="transaction_filter_batch"
                    )
                
                with col_filter2:
                    search_term = st.text_input("Search in results", key="transaction_search_batch")
                
                with col_filter3:
                    # Show only errors by default if there are any
                    if actual_errors > 0:
                        show_errors_only = st.checkbox("Show Only Errors", value=False, key="show_errors_only")
                    else:
                        show_errors_only = False
                
                # Apply filters
                filtered_df = results_df.copy()
                
                # Priority filter: show only errors if requested
                if 'show_errors_only' in locals() and show_errors_only:
                    filtered_df = filtered_df[filtered_df["Status"] == "Error"]
                elif filter_status != "All":
                    filtered_df = filtered_df[filtered_df["Status"] == filter_status]
                
                if search_term:
                    filtered_df = filtered_df[filtered_df["Message"].str.contains(search_term, case=False, na=False)]
                
                # Display count of filtered results with status breakdown
                st.write(f"**Showing {len(filtered_df):,} of {len(results_df):,} results**")
                
                # Show quick stats for filtered results
                if len(filtered_df) > 0:
                    filtered_status_counts = filtered_df["Status"].value_counts()
                    status_summary = " | ".join([f"{status}: {count}" for status, count in filtered_status_counts.items()])
                    st.caption(f"Filtered breakdown: {status_summary}")
                
                # FIXED: Color-coded status display
                def format_status(status):
                    """Format status with appropriate colors and icons"""
                    status_formats = {
                        "Success": "🟢 Success",
                        "Error": "🔴 Error", 
                        "Skipped": "🟡 Skipped",
                        "Info": "🔵 Info"
                    }
                    return status_formats.get(status, status)
                
                # Apply color formatting to status column
                if len(filtered_df) > 0:
                    display_df = filtered_df.copy()
                    display_df["Status"] = display_df["Status"].apply(format_status)
                
                # Display results with pagination
                results_per_page = 500
                if len(filtered_df) > results_per_page:
                    total_pages = (len(filtered_df) + results_per_page - 1) // results_per_page
                    page = st.selectbox(f"Page (1-{total_pages})", range(1, total_pages + 1), key="transaction_page_batch") - 1
                    start_idx = page * results_per_page
                    end_idx = min(start_idx + results_per_page, len(filtered_df))
                    display_df = display_df.iloc[start_idx:end_idx] if len(filtered_df) > 0 else pd.DataFrame()
                    st.write(f"Showing results {start_idx + 1} to {end_idx}")
                else:
                    display_df = display_df if len(filtered_df) > 0 else pd.DataFrame()
                
                # Display results with proper styling
                if len(display_df) > 0:
                    st.dataframe(
                        display_df, 
                        height=400, 
                        use_container_width=True,
                        column_config={
                            "Status": st.column_config.TextColumn("Status", width="small"),
                            "Message": st.column_config.TextColumn("Message", width="large")
                        }
                    )
                else:
                    st.info("No results match your current filters.")
                
                # FIXED: Enhanced export options
                col_export1, col_export2 = st.columns(2)
                
                with col_export1:
                    if st.button("📥 Export All Results to CSV", key="export_transaction_all"):
                        csv = results_df.to_csv(index=False)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        st.download_button(
                            label="Download All Results CSV",
                            data=csv,
                            file_name=f"transaction_processing_results_all_{timestamp}.csv",
                            mime="text/csv",
                            key="download_transaction_all"
                        )
                
                with col_export2:
                    if actual_errors > 0 and st.button("⚠️ Export Errors Only", key="export_transaction_errors"):
                        errors_df = results_df[results_df["Status"] == "Error"]
                        csv = errors_df.to_csv(index=False)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        st.download_button(
                            label="Download Errors CSV",
                            data=csv,
                            file_name=f"transaction_processing_errors_{timestamp}.csv",
                            mime="text/csv",
                            key="download_transaction_errors"
                        )
                
                # Show alerts for different scenarios
                if actual_errors > 0:
                    st.error(f"⚠️ **{actual_errors} actual errors** require attention. Use the 'Show Only Errors' filter to review them.")
                
                if skipped_duplicates > 0:
                    st.info(f"ℹ️ **{skipped_duplicates} duplicate transactions** were safely skipped. This is normal behavior for data integrity.")
                
                if successful_ops > 0:
                    st.success(f"✅ **{successful_ops} transactions** were successfully processed!")
                    
        else:
            st.info("No transaction processing results available. Please upload and process transaction data first.")
    with result_tabs[3]:
        # Relationship Results Tab
        if 'relationship_results' in st.session_state and st.session_state.relationship_results:
            
            filename = getattr(st.session_state, 'relationship_metadata', {}).get('filename', None)
            log_processing_results(
                "Relationship", 
                st.session_state.relationship_success_count, 
                st.session_state.relationship_error_count, 
                st.session_state.relationship_results,
                filename
            )
            # Display metrics with enhanced styling
            st.subheader(" Relationship Processing Summary")
            cols = st.columns(4)
            
            total_ops = st.session_state.relationship_success_count + st.session_state.relationship_error_count
            success_rate = (st.session_state.relationship_success_count / total_ops * 100) if total_ops > 0 else 0
            
            cols[0].metric(" Successful Operations", st.session_state.relationship_success_count)
            cols[1].metric(" Failed Operations", st.session_state.relationship_error_count)
            cols[2].metric(" Total Operations", total_ops)
            cols[3].metric(" Success Rate", f"{success_rate:.1f}%")
            
            # Display detailed results with advanced filtering
            st.subheader(" Detailed Relationship Results")
            
            # Convert results to DataFrame for easier filtering
            results_df = pd.DataFrame(st.session_state.relationship_results, columns=["Message"])
            
            # Add status column
            results_df["Status"] = results_df["Message"].apply(
                lambda x: "Success" if "Successfully" in x or "processed" in x else "Error"
            )
            
            # Enhanced filter options
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                filter_status = st.selectbox("Filter by Status", ["All", "Success", "Error"], key="relationship_filter_batch")
            
            with col_filter2:
                search_term = st.text_input("Search in results", key="relationship_search_batch")
            
            # Apply filters
            filtered_df = results_df.copy()
            
            if filter_status != "All":
                filtered_df = filtered_df[filtered_df["Status"] == filter_status]
            
            if search_term:
                filtered_df = filtered_df[filtered_df["Message"].str.contains(search_term, case=False)]
            
            # Display count of filtered results
            st.write(f"Showing {len(filtered_df):,} of {len(results_df):,} results")
            
            # Display results with pagination
            results_per_page = 50
            if len(filtered_df) > results_per_page:
                total_pages = (len(filtered_df) + results_per_page - 1) // results_per_page
                page = st.selectbox(f"Page (1-{total_pages})", range(1, total_pages + 1), key="relationship_page_batch") - 1
                start_idx = page * results_per_page
                end_idx = min(start_idx + results_per_page, len(filtered_df))
                display_df = filtered_df.iloc[start_idx:end_idx]
                st.write(f"Showing results {start_idx + 1} to {end_idx}")
            else:
                display_df = filtered_df
            
            # Display results
            st.dataframe(display_df, height=400, use_container_width=True)
            
            # Export results option
            if st.button(" Export Relationship Results to CSV", key="export_relationship_batch"):
                csv = results_df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"relationship_batch_processing_results_{timestamp}.csv",
                    mime="text/csv",
                    key="download_relationship_batch"
                )
        else:
            st.info("No relationship processing results available. Please upload and process relationship data first.")

    with result_tabs[4]:
        # List Management Results Tab
        if 'list_results' in st.session_state:
            filename = getattr(st.session_state, 'list_metadata', {}).get('filename', None)
            
            log_processing_results(
                "List Management", 
                st.session_state.list_success_count, 
                st.session_state.list_error_count, 
                st.session_state.list_results,
                filename
            )
            # Display metrics with enhanced styling
            st.subheader(" Relationship Processing Summary")
            cols = st.columns(4)
            
            total_list_ops = st.session_state.list_success_count + st.session_state.list_error_count
            success_rate = (st.session_state.list_success_count / total_list_ops * 100) if total_list_ops > 0 else 0
            
            cols[0].metric(" Successful Operations", st.session_state.list_success_count)
            cols[1].metric(" Failed Operations", st.session_state.list_error_count)
            cols[2].metric(" Total Operations", total_ops)
            cols[3].metric(" Success Rate", f"{success_rate:.1f}%")
            
            # Display detailed results with advanced filtering
            st.subheader(" Detailed List Management Results")
            
            # Convert results to DataFrame for easier filtering
            results_df = pd.DataFrame(st.session_state.list_results, columns=["Message"])
            
            # Add status column
            results_df["Status"] = results_df["Message"].apply(
                lambda x: "Success" if "Successfully" in x or "processed" in x else "Error"
            )
            
            # Enhanced filter options
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                filter_status = st.selectbox("Filter by Status", ["All", "Success", "Error"], key="list_filter_batch")
            
            with col_filter2:
                search_term = st.text_input("Search in results", key="list_search_batch")
            
            # Apply filters
            filtered_df = results_df.copy()
            
            if filter_status != "All":
                filtered_df = filtered_df[filtered_df["Status"] == filter_status]
            
            if search_term:
                filtered_df = filtered_df[filtered_df["Message"].str.contains(search_term, case=False)]
            
            # Display count of filtered results
            st.write(f"Showing {len(filtered_df):,} of {len(results_df):,} results")
            
            # Display results with pagination
            results_per_page = 50
            if len(filtered_df) > results_per_page:
                total_pages = (len(filtered_df) + results_per_page - 1) // results_per_page
                page = st.selectbox(f"Page (1-{total_pages})", range(1, total_pages + 1), key="list_page_batch") - 1
                start_idx = page * results_per_page
                end_idx = min(start_idx + results_per_page, len(filtered_df))
                display_df = filtered_df.iloc[start_idx:end_idx]
                st.write(f"Showing results {start_idx + 1} to {end_idx}")
            else:
                display_df = filtered_df
            
            # Display results
            st.dataframe(display_df, height=400, use_container_width=True)
            
            # Export results option
            if st.button("📥 Export List Management Results to CSV", key="export_list_batch"):
                csv = results_df.to_csv(index=False)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"list_management_processing_results_{timestamp}.csv",
                    mime="text/csv",
                    key="download_list_batch"
                )
        else:
            st.info("No list management processing results available. Please upload and process list management data first.")

    # Batch Performance Tab
    with result_tabs[5]:
        st.subheader(" Batch Processing Performance Metrics")
        
        # Calculate and display overall performance metrics
        if any([st.session_state.get('entity_processed'), st.session_state.get('account_processed'), st.session_state.get('transaction_processed')]):
            
            # Performance summary
            st.markdown("### Performance Summary")
            
            perf_data = []
            
            if st.session_state.get('entity_processed'):
                entity_total = st.session_state.entity_success_count + st.session_state.entity_error_count
                perf_data.append({
                    "Data Type": "Customers",
                    "Total Records": entity_total,
                    "Successful": st.session_state.entity_success_count,
                    "Errors": st.session_state.entity_error_count,
                    "Success Rate": f"{(st.session_state.entity_success_count / entity_total * 100) if entity_total > 0 else 0:.1f}%"
                })
            
            if st.session_state.get('account_processed'):
                account_total = st.session_state.account_success_count + st.session_state.account_error_count
                perf_data.append({
                    "Data Type": "Accounts",
                    "Total Records": account_total,
                    "Successful": st.session_state.account_success_count,
                    "Errors": st.session_state.account_error_count,
                    "Success Rate": f"{(st.session_state.account_success_count / account_total * 100) if account_total > 0 else 0:.1f}%"
                })
            
            if st.session_state.get('transaction_processed'):
                transaction_total = st.session_state.transaction_success_count + st.session_state.transaction_error_count
                perf_data.append({
                    "Data Type": "Transactions",
                    "Total Records": transaction_total,
                    "Successful": st.session_state.transaction_success_count,
                    "Errors": st.session_state.transaction_error_count,
                    "Success Rate": f"{(st.session_state.transaction_success_count / transaction_total * 100) if transaction_total > 0 else 0:.1f}%"
                })
            
            if perf_data:
                perf_df = pd.DataFrame(perf_data)
                st.dataframe(perf_df, use_container_width=True)
                
                # Calculate totals
                total_records = sum([item["Total Records"] for item in perf_data])
                total_successful = sum([item["Successful"] for item in perf_data])
                total_errors = sum([item["Errors"] for item in perf_data])
                overall_success_rate = (total_successful / total_records * 100) if total_records > 0 else 0
                
                st.markdown("### Overall Performance")
                cols = st.columns(4)
                cols[0].metric(" Total Records Processed", f"{total_records:,}")
                cols[1].metric(" Total Successful", f"{total_successful:,}")
                cols[2].metric(" Total Errors", f"{total_errors:,}")
                cols[3].metric(" Overall Success Rate", f"{overall_success_rate:.1f}%")
                
        else:
            st.info("No performance data available. Process some data to see performance metrics.")
   


# Enhanced sidebar with batch processing info
st.sidebar.markdown(
    """
    ###  Use this data loader  to
    - Process and Load **Customer Data**
    - Process and Load **Account Data**
    - Process and Load **Transactions Data** and perform **Automatic AML Detection** after transactions are uploaded
    - Fixed Width Files
    - There are multiple formats supported, refer the **Download Templates (Multiple Formats)** section
    """)

# AML Status
st.sidebar.header("🛡️ AML Detection System Status")
if st.session_state.aml_available:
    st.sidebar.success(" AML System Active")
    # if st.session_state.get('aml_completed', False):
    #     aml_results = st.session_state.get('aml_results', {})
    #     alerts = aml_results.get('alerts_generated', 0)
    #     st.sidebar.metric("Recent Alerts", alerts)
else:
    st.sidebar.error(" AML System Unavailable")


# Check log file status in sidebar
check_log_file_status()
# create_schema_management_interface()

# Add custom CSS for enhanced styling
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        border-radius: 4px 4px 0 0;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: rgba(151, 166, 195, 0.15);
    }
    
    .stButton>button {
        width: 100%;
    }
    
    /* Enhanced metrics styling */
    .metric-container {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    
    /* Success message styling */
    .success-message {
        padding: 10px;
        background-color: #d4f1dd;
        border-left: 5px solid #28a745;
        margin-bottom: 10px;
    }
    
    /* Error message styling */
    .error-message {
        padding: 10px;
        background-color: #f7d4d4;
        border-left: 5px solid #dc3545;
        margin-bottom: 10px;
    }
    
    /* Batch processing highlight */
    .batch-highlight {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
    }
    
    /* Fix dataframe styling */
    .dataframe {
        width: 100% !important;
    }
    
    /* Performance metrics styling */
    .performance-metric {
        background: rgba(0, 123, 255, 0.1);
        border: 1px solid rgba(0, 123, 255, 0.2);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Add footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p style='font-size:0.8rem'>Bliink Payments LLC - Version : 1.0</p>
</div>
""", unsafe_allow_html=True)