def fetch_pl(folder, prefix):
    dfs = []
    for c in range(1, totalCount, 1000):
        queryargs = { 'query' : 'SELECT * FROM ProfitAndLoss STARTPOSITION {c} MAXRESULTS 1000'.format(c) }
        url = preurl + urlencode(queryargs) 
        response = requests.post(url, headers=reqheaders)
        print(response.status_code)
        rawdata = json.loads(response.content) # comes as text, not file
        # once we have the JSON, we need to parse through it.
        profit_and_loss(rawdata)
        dfs.append(what?)
    
    
def profit_and_loss(rawdata):
child_count = 0
for child in child_count:
    child_df = normalize(data['Rows.Row'][{row_num}])
    child_count += 1
    if not 'Rows.Row' in child_df:
        if 'Header.ColData' in child_df:
            for row in rows: # best way to iterate through rows?
                child_row = normalize(data['Header.ColData'][{row}])
                if child_row[1]['value'] == 0:
                    pass
                else:
                    # add account name, account Id, and value to df
        elif data has column 'ColData':
            for row in rows:
                child_row = normalize(data['ColData'][{row}]
                # add account name, account Id, and value to df
    else:
            profit_and_loss(data)
            
# how to keep track of which child level we are at?