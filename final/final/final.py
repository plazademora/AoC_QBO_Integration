import pandas as pd
import requests
import json
import re
import os

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
preurl  = '{0}/v3/company/{1}/query?'.format(baseurl, realmid)
token   = 'Bearer {0}'.format(accesstoken)

reqheaders = {
    'Authorization' : token,
    'Accept' : 'application/json'
}
######################################################################
### STILL NEED TO ADD REALMID COLUMN TO EACH END-RESULT DATA FRAME ###
######################################################################

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
        av = "NULL"
    
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
                return_value = json_crawler_pal_bs_dyno(row_data)
                lst = lst + return_value
            except:
                pass

        return lst

def fetch_pal_bs(report, folder, filename):
    """
    Fetch appropriate report, parse data into dataframe, save to ???
    """
    dfs = []
    # Iterate through all RealmIDs to fetch reports
    for c in range(1, totalCount, 1000):
        queryargs = {
            'query' : 'SELECT * FROM {0} STARTPOSITION {1} MAXRESULTS 1000'.format(report, c)
        }
        # Request reports
        url = preurl + urlencode(queryargs)
        response = requests.post(url, headers=reqheaders)
        print(response.status_code)
        rawdata = json.loads(response.content)
        # Parse report data
        dfs.append(json_crawler_pal_bs(rawdata))
    finaldf = pandas.concat(dfs)
    finaldf['RealmID'] = [realmid] * len(finaldf)
    fullpath = "{0}/{1}.pk1".format(folder, filename)
    os.makedirs(os.path.dirname(fullpath), exist_ok=True)
    finaldf.to_pickle(fullpath)    

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
                    vals[k] = "NULL"
            
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

def fetch_tl(report, folder, filename):
    """
    Fetch appropriate report, parse data into dataframe, save to ???
    """
    dfs = []
    # Iterate through all RealmIDs to fetch reports
    for c in range(1, totalCount, 1000):
        queryargs = {
            'query' : 'SELECT * FROM {0} STARTPOSITION {1} MAXRESULTS 1000'.format(report, c)
        }
        # Request reports
        url = preurl + urlencode(queryargs)
        response = requests.post(url, headers=reqheaders)
        print(response.status_code)
        rawdata = json.loads(response.content)
        # Parse report data
        dfs.append(json_crawler_ts(rawdata))
    finaldf = pandas.concat(dfs)
    finaldf['RealmID'] = [realmid] * len(finaldf)
    fullpath = "{0}/{1}.pk1".format(folder, filename)
    os.makedirs(os.path.dirname(fullpath), exist_ok=True)
    finaldf.to_pickle(fullpath)    

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
            td   = data['value'][0]
            tt   = data['value'][1]
            ttid = data['id'][1]
            dn   = data['value'][2]
            a    = data['value'][3]
            vn   = data['value'][4]
            vid  = data['id'][4]
            m    = data['value'][5]
            s    = data['value'][6]
            sid  = data['id'][6]
            ta   = data['value'][7]
            nab  = data['value'][8]
            
            vals = {
                'AccountName' : "",
                'AccountID' : "",
                'BeginningBalance' : "",
                'TransactionDate' : td,
                'TransactionType' : tt,
                'TransactionTypeID' : ttid,
                'DocumentNumber' : dn,
                'Adjustment' : a,
                'VendorName' : vn,
                'VendorID' : vid,
                'Memo' : m,
                'Split' : s,
                'SplitID' : sid,
                'TransactionAmount' : ta,
                'NewAccountBalance' : nab
            }
            # Replace missing values           
            for k in vals:
                if not vals[k]:
                    vals[k] = "NULL"
            
            return vals

        # Beginning balance data frame
        elif data['value'][0] == "Beginning Balance":
            bb   = data['value'][8]

            return {
                'BeginningBalance' : bb
            }

        # Name data frame
        else:
            # Check for absent Account ID and impute NULL if return False
            if 'id' in data:
                an  = data['value'][0]
                aid = data['id'][0]
            else:
                an  = data['value'][0]
                aid = "NULL"
                
            return {
                'AccountName' : an,
                'AccountID' : aid,
                'BeginningBalance' : "",
                'TransactionDate' : "",
                'TransactionType' : "",
                'TransactionTypeID' : "",
                'DocumentNumber' : "",
                'Adjustment' : "",
                'VendorName' : "",
                'VendorID' : "",
                'Memo' : "",
                'Split' : "",
                'SplitID' : "",
                'TransactionAmount' : "",
                'NewAccountBalance' : ""
            } 

def assign_col_data(coldata, end):
    """
    Assign values parsed from transaction detail object to global row_dict var.
    """
    if coldata['TransactionDate'] == "":
        end['BeginningBalance'] = coldata['BeginningBalance']
    else:
        keys = [
            'TransactionDate',
            'TransactionType',
            'TransactionTypeID',
            'DocumentNumber',
            'Adjustment',
            'VendorName',
            'VendorID',
            'Memo',
            'Split',
            'SplitID',
            'TransactionAmount',
            'NewAccountBalance'
        ]
        
        for i in range(len(keys)):
            end[keys[i]] = coldata[keys[i]]

    return end

def impute_null(dct):
    """
    Impute null values where no transaction detail exists.
    """
    for k in dct:
        if not dct[k]:
            dct[k] = "NULL"
    return dct

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
        data = pd.json_normalize(data)
        return_value = json_crawler_gl(data)
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
            except:
                pass
            # Normalize by 'ColData'
            try:
                record_data = pd.json_normalize(data['ColData'][row])
                if row == 0:
                    row_dict['BeginningBalance'] = parse_record_gl(record_data)['BeginningBalance']
                else:
                    new_dict = dict(assign_col_data(parse_record_gl(record_data), row_dict))
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

def fetch_tl(report, folder, filename):
    """
    Fetch appropriate report, parse data into dataframe, save to ???
    """
    dfs = []
    # Iterate through all RealmIDs to fetch reports
    for c in range(1, totalCount, 1000):
        queryargs = {
            'query' : 'SELECT * FROM {0} STARTPOSITION {1} MAXRESULTS 1000'.format(report, c)
        }
        # Request reports
        url = preurl + urlencode(queryargs)
        response = requests.post(url, headers=reqheaders)
        print(response.status_code)
        rawdata = json.loads(response.content)
        # Parse report data
        dfs.append(json_crawler_gl(rawdata))
    finaldf = pandas.concat(dfs)
    finaldf['RealmID'] = [realmid] * len(finaldf)
    fullpath = "{0}/{1}.pk1".format(folder, filename)
    os.makedirs(os.path.dirname(fullpath), exist_ok=True)
    finaldf.to_pickle(fullpath)   
