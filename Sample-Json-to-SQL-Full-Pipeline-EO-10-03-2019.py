import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import re
from datetime import date

### Action Pipeline ###

# This is an example of a live ETL script I made which transferred bank data packaged in a JSON file into an Azure-based SQL Server
# The ETL accounts for omitted tables in the data. It also accounts for what type of form is sent, based on one of 4 configurations.
# The service I build this ETL for has since been discontinued, and the credentials/password variables have been scrubbed.
# This is provided strictly as a sample of my Data Engineering, and is not to be used without my (Eric Oulster's) expressed permission

pw_eng = 'this is where the SQL driver would go'  # This is where the engine password would go


def recpipe(json_file):
    data_dict = pd.read_json(json_file, typ='series')
    reccomendation_action = pd.DataFrame(data_dict).T    
    # mysql engine
    engine = create_engine(pw_eng)
    
    # sending to mssql
    
    try:
        reccomendation_action.to_sql(name='reccomendation_action', con=engine, schema='sample_main', if_exists='append')
    except:
        print("reccomendation_action not processing")
### Bank Scrape Pipeline ###

def bankpipe(json_file):
    
    sf_id = str(json_file)[:-17]
    
    data_dict = pd.read_json(json_file, typ='series')
    
    # bank scrape_info #
    
    try:
    
        bank_scrape_info = data_dict.drop(index=['accounts', 'contacts'])
    
        bank_scrape_info = pd.DataFrame(bank_scrape_info).T
    
        bank_scrape_info['SF_ID'] = sf_id
    
        bank_scrape_info['Report_date'] = data_dict['complete_datetime']
    
        print(bank_scrape_info)
    except:
        print("bank_scrape info not processing")
        pass
    
    try:
        misc_contact = pd.DataFrame(data_dict['contacts'])
    
        misc_contact['name'] = data_dict['name']
    
        misc_contact['SF_ID'] = sf_id
        misc_contact['Report_date'] = data_dict['complete_datetime']
        print(misc_contact)
    except:
        print("no misc_contact")
        pass
    
        #### Accounts ####
        
    try:
        
            bank_account = pd.DataFrame(data_dict['accounts'])
    
            # initializing empty lists
    
            mean_closing_balance_30_list = [] 
            mean_closing_balance_list = []
            # there are several accounts, this nested loop allows me to access all of them in a fell swoop
    
            for x in data_dict['accounts']:
                lvl_1 = x
                for key, value in lvl_1['statistics'].items():
                    if key == 'mean_closing_balance_30':
                        mean_closing_balance_30_list.append(value)
                    elif key == 'mean_closing_balance':
                        mean_closing_balance_list.append(value)
                    else:
                        pass
                # that's a placeholder for future values
    
            bank_account['mean_close'] = mean_closing_balance_list
            bank_account['mean_close_30'] = mean_closing_balance_30_list
            bank_account.drop(columns=['statistics'], inplace=True)
            bank_account['mask_id'] = str("XXXX" + bank_account['account'][3:])    
            bank_account['SF_ID'] = sf_id
            bank_account['Report_date'] = data_dict['complete_datetime']
    
            bank_account.drop('transactions', axis=1, inplace=True)
    
            print(bank_account)
    except:
            print("Account information not processed.")
            pass
        
    ### Making transactions column ###
    
    try:
        transactions = pd.DataFrame()
    
        account_id = []
        for x in data_dict['accounts']:
            h = pd.DataFrame(x['transactions'])
             # adding the account it's from to the list
            h['account_id'] = str(x['account'])
            h['mask_id'] = str("XXXX" + x['account'][3:])
            h['sf_id'] = sf_id
            h['Report_Date'] = str(data_dict['complete_datetime'])
                    # sort=false doesn't work in my version of pandas. Add later
            transactions = transactions.append(h, ignore_index=True)
    
    
            transactions['flags'] = transactions.flags.astype(str)
            print(transactions)
    except:
        print("No Transaction information was processed.")
        pass
    
    # connection engine
    
    engine = create_engine(pw_eng)
    
    try:
        bank_scrape_info.to_sql(name='bank_scrape_info', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bank scrape info not pushed")
    try:
        misc_contact.to_sql(name='misc_contact', con=engine, schema='sample_main', if_exists='append')
    except:
        print("misc contact info not pushed")
    try:
        bank_account.to_sql(name='bank_account', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bank account info not pushed")
    try:
        transactions.to_sql(name='transactions', con=engine, schema='sample_main', if_exists='append')
    except:
        print("transaction info not pushed")
        
### End Bank Scrape Pipeline ###

####### Credit Report Pipeline Start #######


def creditpipe(json_file):

    sf_id = json_file[:-19]
    
    data_dict = pd.read_json(json_file, typ='series')
    
    # time processing #
    
    datetime = str(data_dict['Date'])[0:4] + '-' + str(data_dict['Date'])[4:6] + '-' + str(data_dict['Date'])[6:8] + ' ' + str(data_dict['Time'])[0:2] + ':' + str(data_dict['Time'])[2:4] + '.' + str(data_dict['Time'])[4:6]
    
    ### Credit Report ###
    try:
        base_credit = data_dict.drop(index=['TU_FFR_Report'])
    
        base_credit = pd.DataFrame(base_credit).T
    
    
        base_credit['SF_ID'] = sf_id
        base_credit['Credit_Member_ID'] = base_credit['MemberCode']
        base_credit['TU_FFR_HIT'] = data_dict['TU_FFR_Report'][0]['Hit']
        base_credit['Report_Date'] = datetime
        try:
            base_credit['FFR_filedate'] = data_dict['TU_FFR_Report'][0]['OnFileDate']
        except:
            base_credit['FFR_filedate'] = np.nan
        print(base_credit)
    
    
    except:
        print("No transaction information was processed.")
        pass
    
    # Bankruptcies #
    try:
        bankruptcy = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Bankruptcies'])
        bankruptcy['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        bankruptcy['Report_Date'] = datetime
        print(bankruptcy)
    except:
        print("No bankruptcy information was processed")
        
    # Trades #
    try:
        trades = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Trades'])
    
    
        trades['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        trades['Report_Date'] = datetime
    
        print(trades)
    except:
        print("No Trade Information Processed")
        
    # Credit Summary #
    try:
        credit_summary = pd.DataFrame(data_dict['TU_FFR_Report'][0]['CreditSummary'], index=[0])
        credit_summary['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        credit_summary['Report_Date'] = datetime
        print(credit_summary)
    except:
        print("No credit summary information processed")
        
    # Credit Summary Details #
    
    try:
        credit_details = pd.DataFrame(data_dict['TU_FFR_Report'][0]['CreditSummaryDetails'])
    
        credit_details['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        credit_details['Report_Date'] = datetime
        print(credit_details)
    except:
        print("Detailed credit summary info not found.")
        
    # Score Products #
    try:
        score_products = pd.DataFrame(data_dict['TU_FFR_Report'][0]['ScoreProducts'])
        score_products['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        score_products['Report_Date'] = datetime
        print(score_products)
    except:
        print("Product Score not found.")
        
    # bankings
    try:
        bankings = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Bankings'])
        bankings['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        bankings['Report_Date'] = datetime
        print(bankings)
    except:
        print('No bankings found')
        
    # employments
    try:
        employments = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Employments'])
        employments['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        employments['Report_Date'] = datetime
        print(employments)
    except:
        print("No employments found")
        
    # collections
    try:
        collections = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Collections'])
        collections['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        collections['Report_Date'] = datetime
        print(collections)
    except:
        print("collections not found")
        
    # Inquiries
    try:
        inquiries = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Inquiries'])
        inquiries['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        inquiries['Report_Date'] = datetime
        print(inquiries)
    except:
        print("Inquiries not found")
    
    # Legals
    try:
        legals = pd.DataFrame(data_dict['TU_FFR_Report'][0]['Legals'])
        legals['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        legals['Report_Date'] = datetime
        print(legals)
    except:
        print("Legals not found")
        
    # Consumer Statements
    try:
        consumer_statements = pd.DataFrame(data_dict['TU_FFR_Report'][0]['ConsumerStatements'])
        consumer_statements['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        consumer_statements['Report_Date'] = datetime
        print(consumer_statements)
    except:
        print("Consumer_statements not found")
        
    # Miscellaneous Statements
    try:
        misc_statements = pd.DataFrame(data_dict['TU_FFR_Report'][0]['MiscellaneousStatements'])
        misc_statements['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        misc_statements['Report_Date'] = datetime
        print(misc_statements)
    except:
        print("misc_statements not found")
        
    # registered items Statements
    try:
        reg_items = pd.DataFrame(data_dict['TU_FFR_Report'][0]['RegisteredItems'])
        reg_items['Credit_Member_ID'] = str(base_credit['MemberCode'].values)[2:-2]
        reg_items['Report_Date'] = datetime
        print(reg_items)
    except:
        print("reg_items not found")
        
    engine = create_engine(pw_eng)
    
    try:
        base_credit.to_sql(name='base_credit', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bank scrape info not pushed")
    try:
        bankruptcy.to_sql(name='bankruptcy', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bankruptcy info not pushed")
    try:
        trades.to_sql(name='trades', con=engine, schema='sample_main', if_exists='append')
    except:
        print("trades info not pushed")
    try:
        credit_summary.to_sql(name='credit_summary', con=engine, schema='sample_main', if_exists='append')
    except:
        print("credit_summary info not pushed")
    try:
        credit_details.to_sql(name='credit_details', con=engine, schema='sample_main', if_exists='append')
    except:
        print("credit_details info not pushed")
    # New stuff ###
    try:
        score_products.to_sql(name='score_products', con=engine, schema='sample_main', if_exists='append')
    except:
        print("score_products info not pushed")
    try:
        bankings.to_sql(name='bankings', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bankings info not pushed")
    try:
        employments.to_sql(name='employments', con=engine, schema='sample_main', if_exists='append')
    except:
        print("employments info not pushed")
    try:
        collections.to_sql(name='collections', con=engine, schema='sample_main', if_exists='append')
    except:
        print("collections info not pushed")
    try:
        inquiries.to_sql(name='inquiries', con=engine, schema='sample_main', if_exists='append')
    except:
        print("inquiries info not pushed")
    try:
        legals.to_sql(name='legals', con=engine, schema='sample_main', if_exists='append')
    except:
        print("legals info not pushed")
    try:
        consumer_statements.to_sql(name='consumer_statements', con=engine, schema='sample_main', if_exists='append')
    except:
        print("consumer_statements info not pushed")
    try:
        misc_statements.to_sql(name='misc_statements', con=engine, schema='sample_main', if_exists='append')
    except:
        print("misc_statements info not pushed")
    try:
        reg_items.to_sql(name='reg_items', con=engine, schema='sample_main', if_exists='append')
    except:
        print("reg_items info not pushed")
        
####### Credit Report Pipeline End #########

######## PRIMARY JSON PIPELINE ########

def jsonpipe(json_file):

    data_dict = pd.read_json(json_file, typ='series')

    # customer name - for master table

    try:
        customer_name = data_dict['BankScrapeData']['name']
    except:
        try:
            customer_name = str(data_dict['CustomerInformation']['FirstName']) + " " + str(data_dict['CustomerInformation']['LastName'])
        except:
            try:
                customer_name = str(data_dict['CreditReportData']['TU_FFR_Report'][0]['Names']['FirstName']) + " " + str(data_dict['CreditReportData']['TU_FFR_Report'][0]['Names']['LastName'])
            except:
                customer_name = "Not specified"

    #### Making master_table ####

    master_table = pd.DataFrame(data_dict)

    master_table = master_table.T

    ## make a name value ##

    master_table['name'] = customer_name

    try:   
        master_table['Credit_Member_ID'] = data_dict['CreditReportData']['MemberCode']
    except:   
        master_table['Credit_Member_ID'] = "Not found"

    master_table.drop(['BankScrapeData', 'CustomerInformation', 'CreditReportData', 'Recommendations'], axis=1, inplace=True)

    #### Making customerinfo_DF ####

    customer_info = pd.DataFrame(data_dict['CustomerInformation'], index=[0])

    customer_info['SF_ID'] = data_dict['SalesforceID']
    customer_info['Report_date'] = data_dict['CreatedOnDate']

    ### Contact from the TU_FFR - less desirable for use than 'contactinfo' data, but a good contingency.

    try:
        misc_contact = pd.DataFrame(data_dict['BankScrapeData']['contacts'])

        misc_contact['name'] = data_dict['BankScrapeData']['name']

        misc_contact['SF_ID'] = data_dict['SalesforceID']
        misc_contact['Report_date'] = data_dict['CreatedOnDate']
        print(misc_contact)
    except:
        print("no misc_contact")
        pass


    ### bank scrape_info ###
    
    try:
        bank_scrape_info = pd.DataFrame.from_dict(data_dict['BankScrapeData'], orient='index').T
    
        bank_scrape_info.drop(columns=['accounts','contacts'], inplace=True)
    
        bank_scrape_info['SF_ID'] = data_dict['SalesforceID']
    
        bank_scrape_info['Report_date'] = data_dict['CreatedOnDate']
    
        print(bank_scrape_info)
    except:
        print("General bank scrape info not found")
        pass


        #### Accounts ####
        
    try:
        
            bank_account = pd.DataFrame(data_dict['BankScrapeData']['accounts'])

            # initializing empty lists

            mean_closing_balance_30_list = [] 
            mean_closing_balance_list = []
            # there are several accounts, this nested loop allows me to access all of them in a fell swoop

            for x in data_dict['BankScrapeData']['accounts']:
                lvl_1 = x
                for key, value in lvl_1['statistics'].items():
                    if key == 'mean_closing_balance_30':
                        mean_closing_balance_30_list.append(value)
                    elif key == 'mean_closing_balance':
                        mean_closing_balance_list.append(value)
                    else:
                        pass
                # that's a placeholder for future values

            bank_account['mean_close'] = mean_closing_balance_list
            bank_account['mean_close_30'] = mean_closing_balance_30_list
            bank_account.drop(columns=['statistics'], inplace=True)
            bank_account['mask_id'] = str("XXXX" + bank_account['account'][3:])
            bank_account['SF_ID'] = data_dict['SalesforceID']
            bank_account['Report_date'] = data_dict['CreatedOnDate']

            bank_account.drop('transactions', axis=1, inplace=True)

            print(bank_account)
    except:
            print("Account information not processed. Either data is not present or is incompatible with processes.")
            pass

    ### Making transactions column ###


    try:
        transactions = pd.DataFrame()
        # Remember you need the transaction ID
        account_id = []
        for x in data_dict['BankScrapeData']['accounts']:
                    h = pd.DataFrame(x['transactions'])
                    # adding the account it's from to the list
                    h['account_id'] = str(x['account'])
                    h['Report_Date'] = str(data_dict['CreatedOnDate'])
                    h['mask_id'] = str("XXXX" + x['account'][3:])
                    h['sf_id'] = str(data_dict['SalesforceID'])
                    transactions = transactions.append(h, ignore_index=True)
                    transactions['flags'] = transactions.flags.astype(str)
        print(transactions)
    except:
        print("No Transaction information was processed.")
        pass

    ### Credit Report ###

    # base_credit_report = 
    try:
        base_credit = pd.DataFrame(data_dict['CreditReportData'])

        base_credit.drop(['TU_FFR_Report'], inplace=True, axis=1)

        base_credit['SF_ID'] = data_dict['SalesforceID']
        base_credit['Credit_Member_ID'] = master_table['Credit_Member_ID']
        base_credit['TU_FFR_HIT'] = data_dict['CreditReportData']['TU_FFR_Report'][0]['Hit']
        base_credit['Report_Date'] = data_dict['CreatedOnDate']
        try:
            base_credit['FFR_filedate'] = data_dict['CreditReportData']['TU_FFR_Report'][0]['OnFileDate']
        except:
            base_credit['FFR_filedate'] = np.nan
        print(base_credit)
    except:
        print("No transaction information was processed.")
        pass

    ### TTU_FFR - THIS IS ACTUALLY SEVERAL TABLES ###
    

    # Bankruptcies #
    try:
        bankruptcy = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Bankruptcies'])
        bankruptcy['SF_ID'] = data_dict['SalesforceID']
        bankruptcy['Report_Date'] = data_dict['CreatedOnDate']

        print(bankruptcy)
    except:
        print("No bankruptcy information was processed")

    # Trades #
    try:
        trades = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Trades'])


        trades['SF_ID'] = str(master_table['SalesforceID'].values)[2:-2]
        trades['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]

        print(trades)
    except:
        print("No Trade Information Processed")

    # Credit Summary #
    try:
        credit_summary = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['CreditSummary'], index=[0])
        credit_summary['SF_ID'] = data_dict['SalesforceID']
        credit_summary['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(credit_summary)
    except:
        print("No credit summary information processed")

    # Credit Summary Details #

    try:
        credit_details = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['CreditSummaryDetails'])

        credit_details['Credit_Member_ID'] = str(master_table['Credit_Member_ID'].values)[2:-2]
        credit_details['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(credit_details)
    except:
        print("Detailed credit summary info not found.")

    # Score Products #
    try:
        score_products = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['ScoreProducts'])
        score_products['SF_ID'] = data_dict['SalesforceID']
        score_products['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(score_products)
    except:
        print("Product Score found.")
        
    try:
        bankings = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Bankings'])
        bankings['SF_ID'] = data_dict['SalesforceID']
        bankings['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(bankings)
    except:
        print('No bankings found')
    
    # Employments
    try:
        employments = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Employments'])
        employments['SF_ID'] = data_dict['SalesforceID']
        employments['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(employments)
    except:
        print("No employments found")
        
    
    # add collections #
    try:
        collections = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Collections'])
        collections['SF_ID'] = data_dict['SalesforceID']
        collections['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(collections)
    except:
        print("collections not found")    
    # add inquiries #
    try:
        inquiries = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Inquiries'])
        inquiries['SF_ID'] = data_dict['SalesforceID']
        inquiries['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(inquiries)
    except:
        print("Inquiries not found")    
    # add legals #
    try:
        legals = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['Legals'])
        legals['SF_ID'] = data_dict['SalesforceID']
        legals['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(legals)
    except:
        print("Legals not found")    
    # add consumer statements #
    try:
        consumer_statements = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['ConsumerStatements'])
        consumer_statements['SF_ID'] = data_dict['SalesforceID']
        consumer_statements['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(consumer_statements)
    except:
        print("Consumer_statements not found")
    # end consumer statements #
    
    #add misc statements
    try:
        misc_statements = pd.DataFrame(data_dict['CreditReportData']['TU_FFR_Report'][0]['MiscellaneousStatements'])
        misc_statements['SF_ID'] = data_dict['SalesforceID']
        misc_statements['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(misc_statements)
    except:
        print("misc_statements not found") 
    # add registered items #
    try:
        reg_items = pd.DataFrame(data_dict['TU_FFR_Report'][0]['RegisteredItems'])
        reg_items['SF_ID'] = data_dict['SalesforceID']
        reg_items['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(reg_items)
    except:
        print("reg_items not found")
    # end registered items #

    # Reccomendations #
    try:
        reccomendations = pd.DataFrame(data_dict['Recommendations'])
        reccomendations['SF_ID'] = data_dict['SalesforceID']
        reccomendations['Report_Date'] = str(master_table['CreatedOnDate'].values)[2:-2]
        print(reccomendations)
    except:
        print("No reccomendations found.")

    # file stuff

    # file_string = json_file[:-5]

    # 
    engine = create_engine(pw_eng)

    # Create csv's

    try:
        master_table.to_sql(name='master_table', con=engine, schema='sample_main', if_exists='append')
    except:
        print("master_table info not pushed")
        pass
    try:
        customer_info.to_sql(name='customer_info', con=engine, schema='sample_main', if_exists='append')
    except:
        print("customer_info not pushed")
        pass
    try:
        misc_contact.to_sql(name='misc_contact', con=engine, schema='sample_main', if_exists='append')
    except:
        print("misc_contact not pushed")
        pass
    try:
        bank_scrape_info.to_sql(name='bank_scrape_info', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bank_scrape_info not pushed")
        pass
    try:
        bank_account.to_sql(name='bank_account', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bank_account not pushed")
        pass
    try:
        transactions.to_sql(name='transactions', con=engine, schema='sample_main', if_exists='append')
    except:
        print("transactions not pushed")
        pass
    try:
        base_credit.to_sql(name='base_credit', con=engine, schema='sample_main', if_exists='append')
    except:
        print("base_credit not pushed")
        pass
    try:
        bankruptcy.to_sql(name='bankruptcy', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bankruptcy not pushed")
        pass
    try:
        trades.to_sql(name='trades', con=engine, schema='sample_main', if_exists='append')
    except:
        print("trades not pushed")
        pass
    try:
        credit_details.to_sql(name='credit_details', con=engine, schema='sample_main', if_exists='append')
    except:
        print("credit_details not pushed")
        pass
    try:
        credit_summary.to_sql(name='credit_summary', con=engine, schema='sample_main', if_exists='append')
    except:
        print("credit_summary not pushed")
        pass
    try:
        reccomendations.to_sql(name='reccomendations', con=engine, schema='sample_main', if_exists='append')
    except:
        print("reccomendations not pushed")
        pass
    try:
        score_products.to_sql(name='score_products', con=engine, schema='sample_main', if_exists='append')
    except:
        print("score products not pushed")
        pass 
    # new stuff
    try:
        bankings.to_sql(name='bankings', con=engine, schema='sample_main', if_exists='append')
    except:
        print("bankings info not pushed")
    try:
        employments.to_sql(name='employments', con=engine, schema='sample_main', if_exists='append')
    except:
        print("employments info not pushed")
    try:
        collections.to_sql(name='collections', con=engine, schema='sample_main', if_exists='append')
    except:
        print("collections info not pushed")
    try:
        inquiries.to_sql(name='inquiries', con=engine, schema='sample_main', if_exists='append')
    except:
        print("inquiries info not pushed")
    try:
        legals.to_sql(name='legals', con=engine, schema='sample_main', if_exists='append')
    except:
        print("legals info not pushed")
    try:
        consumer_statements.to_sql(name='consumer_statements', con=engine, schema='sample_main', if_exists='append')
    except:
        print("consumer_statements info not pushed")
    try:
        misc_statements.to_sql(name='misc_statements', con=engine, schema='sample_main', if_exists='append')
    except:
        print("misc_statements info not pushed")
    try:
        reg_items.to_sql(name='reg_items', con=engine, schema='sample_main', if_exists='append')
    except:
        print("reg_items info not pushed")    


########### END OF PRIMARY JSON PIPELINE ##############

############################ ITERATION CODE ##########################
from azure.storage.blob import BlockBlobService
import os

# account credentials

# Using bbs, data is transferred from the cloud to a local machine, is sorted using the above functions, is pushed to SQL, then is moved into an archive database.

acct_name = 'accountsample'
key = 'accountkey'

bbs = BlockBlobService(account_name=acct_name, account_key=key)

generator = bbs.list_blobs('bloblake')

path = r'localpath'

blobnames = []

for blob in generator:
    blobnames.append(blob.name)

for blob in blobnames:
    file = open(path + "\\" + blob, 'w')
    bbs.get_blob_to_path('bloblake', blob, path + "\\" + blob)
    file.close()

with os.scandir(path) as it:
    for entry in it:
        # do an if statement for the three processes
        if re.search('_bank_scrape', str(entry)):
            bankpipe(entry)
        elif re.search('_credit_report', str(entry)):
            creditpipe(entry)
        elif re.search('_action', str(entry)):
            recpipe(entry)
        else:
            jsonpipe(entry)

# this moves data into an archive blob storage in azure
for blob in blobnames:
    bbs.create_blob_from_path('blobarchive', blob, path + "\\" + blob)

# delete local files
for blob in blobnames:
    os.remove(path + "\\" + blob)

for blob in blobnames:
    bbs.delete_blob('bloblake', blob)
