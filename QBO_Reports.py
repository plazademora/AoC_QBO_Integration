import pandas as pd
import requests
import json
import re
import os
import argparse
from intuitlib.client import AuthClient
from urllib.parse import urlencode
from datetime import date

try: realmid
except NameError: realmid = None
try: accesstoken
except NameError: accesstoken = None

reports = [
    'ProfitAndLoss', 
    'BalanceSheet', 
    'GeneralLedger', 
    'TransactionList'
]

baseurl = 'https://quickbooks.api.intuit.com'
preurl  = '{0}/v3/company/{1}/reports'.format(baseurl, realmid)
token   = 'Bearer {0}'.format(accesstoken)

reqheaders = {
    'Authorization' : token,
    'Accept' : 'application/json'
}

def fetch_report(report, folder, filename, crawler_func, start_date="2021-07-01", end_date=date.today()):
    """
    Fetch appropriate report, parse data into dataframe, save to ???
    """
    # Iterate through all RealmIDs to fetch reports
    queryargs = {
        'start_date' : start_date,
        'end_date' : end_date,
        'minorversion' : '3'
    }
    # Request reports
    url = '{0}/{1}?{2}'.format(preurl, report, urlencode(queryargs))
    response = requests.get(url, headers=reqheaders)
    print(response.status_code)
    rawdata = json.loads(response.content)
    # Parse report data
    df = pd.DataFrame(crawler_func(rawdata))
    df['RealmID'] = [realmid] * len(df)
    fullpath = "{0}/{1}.pkl".format(folder, filename)
    os.makedirs(os.path.dirname(fullpath), exist_ok=True)
    df.to_pickle(fullpath) 

############################################
### ProfitAndLoss & BalanceSheet Reports ###
############################################

def parse_record_pal_bs(data):
    """
    Parse out desired data from individual data frame row and return values.
    """
    an  = data['value'][0]
    aid = data['id'][0]
    av  = data['value'][1]
    if not av:
        av = float("NaN")
    
    return {
        'AccountName'  : an,
        'AccountID'    : aid,
        'AccountValue' : av
    }

def json_crawler_pal_bs(data):
    """
    Leverage pandas data frames to normalize nested JSON extracts.
    Use recursion to dynamically navigate through account hierarchy.
    Use function parse_record_pal_bs() to parse out individual row data.
    """
    # Flatten json.load() rawdata into initial dataframe
    if isinstance(data, dict):
        data = pd.json_normalize(data)
        return_value = json_crawler_pal_bs(data)
        return return_value 
    # Crawl through JSON file, normalizing by different keys to find account 
    # data and save to lst
    else:
        # Initialize list to hold account data objects
        lst = []
        # Iterate through rows in data frame
        for row in range(data.shape[0]):
            # Normalize by 'Header.ColData'
            try:
                record_data = pd.json_normalize(data['Header.ColData'][row])
                lst.append(parse_record_pal_bs(record_data))
            except:
                pass
            # Normalize by 'ColData'
            try:
                record_data = pd.json_normalize(data['ColData'][row])
                lst.append(parse_record_pal_bs(record_data))
            except:
                pass
            # Normalize by 'Rows.Row'
            try:
                row_data = pd.json_normalize(data['Rows.Row'][row])
                # Capture lst and return value before recursive function call
                return_value = json_crawler_pal_bs(row_data)
                lst = lst + return_value
            except:
                pass

        return lst
   

##############################
### TransactionList Report ###
##############################

def parse_record_tl(data):
    """
    Iterate through ColData or Header.ColData data frame. 
    Check what kind of data each data frame contains.
    Parse each record in data frame accordingly and return values.
    """
    # Regular expression matching date format in transaction detail object
    rex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

    # Check which ColData data frame is being parsed
    for row in range(data.shape[0]):
        # Transaction detail data frame
        if re.search(rex, data['value'][0]):
            td   = data['value'][0]
            tt   = data['value'][1]
            ttid = data['id'][1]
            dn   = data['value'][2]
            p    = data['value'][3]
            vn   = data['value'][4]
            vid  = data['id'][4]
            m    = data['value'][5]
            s    = data['value'][6]
            sid  = data['id'][6]
            ta   = data['value'][7]
            
            vals = {
                'TransactionDate' : td,
                'TransactionType' : tt,
                'TransactionTypeID' : ttid,
                'DocumentNumber' : dn,
                'Posting' : p,
                'VendorName' : vn,
                'VendorID' : vid,
                'Memo' : m,
                'Split' : s,
                'SplitID' : sid,
                'TransactionAmount' : ta
                }
            # Replace missing values           
            for k in vals:
                if not vals[k]:
                    vals[k] = float("NaN")
            
            return vals

def json_crawler_tl(data):
    # If data is dictionary returned by json.load(), then flatten it to initial pandas data frame
    if isinstance(data, dict):
        data = pd.json_normalize(data)
        return_value = json_crawler_tl(data)
        return return_value 
    # Crawl through JSON file, normalizing by different keys to find account data and save to lst
    else:
        # Initialize list to hold account data objects
        lst = []
        acct_names = []
        
        for row in range(data.shape[0]):
            # Normalize by 'ColData'
            try:
                record_data = pd.json_normalize(data['ColData'][row])
                lst.append(parse_record_tl(record_data))
            except:
                pass
            # Normalize by 'Header.ColData'
            try:
                record_data = pd.json_normalize(data['Header.ColData'][row])
                lst.append(parse_record_tl(record_data))
            except:
                pass
            # Normalize by 'Rows.Row'
            try:
                row_data = pd.json_normalize(data['Rows.Row'][row])
                # Capture lst and return value before recursive function call
                return_value = json_crawler_tl(row_data)
                lst = lst + return_value
            except:
                pass

        return lst


############################
### GeneralLedger Report ###
############################

def parse_record_gl(data):
    """
    Iterate through ColData or Header.ColData data frame. 
    Check what kind of data each data frame contains.
    Parse each record in data frame accordingly and return values.
    """
    # Regular expression matching date format
    date_rex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

    # Check which ColData data frame is being parsed
    for row in range(data.shape[0]):
        # Transaction detail data frame
        
        if re.search(date_rex, data['value'][0]):
            vals = ["", "", "",]
            # vals[0] = AccountName
            # vals[1] = AccountID
            # vals[2] = Beginning Balance
            for row in range(data.shape[0]):
                if 'id' in data:
                    val    = data['value'][row]
                    val_id = data['id'][row]
                    vals.append(val)
                    vals.append(val_id)
                else:
                    val = data['value'][row]
                    vals.append(val)
                    vals.append(float("NaN"))
            return vals

        # Beginning balance data frame
        elif data['value'][0] == "Beginning Balance":
            vals = ["", ""]
            begbal = data['value'][len(data)-1]
            vals.append(begbal)
            return vals

        # Name data frame
        else:
            # Check for absent Account ID and impute NULL if return False
            if 'id' not in data:
                vals = [data['value'][0], float("NaN")]
            else:
                vals = [data['value'][0], data['id'][0]]
                
            return vals

def assign_col_data(coldata, end):
    """
    Assign values parsed from transaction detail object to global row_dict var.
    """
    for i in range(3):
        if not coldata[i]:
            coldata[i] = end[i]
        else:
            pass
    
    return coldata

def get_col_names(data):
    first_col_tbl = pd.json_normalize(data['Columns'])
    col_names_tbl = pd.json_normalize(first_col_tbl['Column'][0])
    count = 0
    col_names = ["AccountName", "AccountNameID", "BeginningBalance"]
    for row in range(col_names_tbl.shape[0]):
        col_names.append(col_names_tbl['ColTitle'][row])
        col_names.append(str(col_names_tbl['ColTitle'][row]) + "ID")
        count += 2
    col_names = [col_name.replace(" ", "") for col_name in col_names]
    col_names.insert(0, count + 3)
    return col_names

def impute_null(lst):
    """
    Impute null values where no transaction detail exists.
    """
    for i in range(len(lst)):
        if not lst[i]:
            lst[i] = float("NaN")
        elif type(lst[i]) == float:
            lst[i] = float("NaN")
    return lst

# Initialize container variable for row data
# Will act as global variable for crawler function to store values
# throughout recursions.
row_dict = {}

def json_crawler_gl(data):
    """
    Iterate through ColData or Header.ColData data frame. 
    Check what kind of data each data frame contains.
    Parse each record in data frame accordingly and return values.
    """
    global row_dict

    # If data is dictionary returned by json.load(), then flatten it to initial pandas data frame
    if isinstance(data, dict):
        col_names = get_col_names(data)
        data = pd.json_normalize(data)
        return_value = col_names + json_crawler_gl(data)
        return return_value 
    
    # Crawl through JSON file, normalizing by different keys to find account data and save to lst
    else:
        # Initialize list to hold account data objects
        lst = []
        
        for row in range(data.shape[0]):
            # Normalize by 'Header.ColData'
            try:
                record_data = pd.json_normalize(data['Header.ColData'][row])
                row_dict = impute_null(parse_record_gl(record_data))
                lst.append(row_dict)
                # print(lst)
            # except Exception as e:
            #     print("Line 121", e)
            except:
                pass
            # Normalize by 'ColData'
            try:
                record_data = pd.json_normalize(data['ColData'][row])
                if row == 0:
                    row_dict.append(parse_record_gl(record_data)[2])
                else:
                    # print(parse_record_gl(record_data), row_dict)
                    # print(assign_col_data(parse_record_gl(record_data), row_dict))
                    new_dict = list(impute_null(assign_col_data(parse_record_gl(record_data), row_dict)))
                    lst.append(new_dict)
            except:
                pass
            # Normalize by 'Rows.Row'
            try:
                row_data = pd.json_normalize(data['Rows.Row'][row])
                # Capture lst and return value before recursive function call
                return_value = json_crawler_gl(row_data)
                lst = lst + return_value
            except:
                pass
        return lst

def extract_col_names(lst):
    num_of_col = lst.pop(0)
    col_names  = lst[:(num_of_col)]
    del lst[:(num_of_col)]
    return col_names
    
def norm_length(lst, col_names):
    for i in range(len(lst)):
        if len(lst[i]) != len(col_names):
            for j in range(len(col_names) - len(lst[i])):
                lst[i].append(float("NaN"))
    return lst

def gl_return_output(data):
    lst       = json_crawler_gl(data)
    col_names = extract_col_names(lst)
    output    = norm_length(lst, col_names)
    df        = pd.DataFrame(output, columns=col_names)
    final_obj = pd.DataFrame.to_dict(df, orient="records")
    return final_obj
  
crawlers = {
        'ProfitAndLoss': json_crawler_pal_bs,
        'BalanceSheet' : json_crawler_pal_bs,
        'TransactionList' : json_crawler_tl,
        'GeneralLedger' : gl_return_output,
    }

for report in reports:
    fetch_report(report, f'./tmp/{report}', realmid, crawlers[report])
