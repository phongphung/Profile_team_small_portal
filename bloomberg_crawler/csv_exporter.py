import logging
from unicodecsv import DictWriter
from bloomberg_crawler import db as temp_company_db

def export_csv_output_file(file_name):
    cursor = temp_company_db.mongo_connection(temp_company_db.MONGO_HOST, temp_company_db.MONGO_PORT,
                                              temp_company_db.MONGO_DB, temp_company_db.MONGO_COLLECTION).find()
    bloomberg_crawler_logs = list(cursor)
    if bloomberg_crawler_logs:
        try:
            with open(file_name, 'w') as f:
                output_fieldnames = ["Name", "Exchange", "root_ticker", "Bloomberg_ticker",
                              "Reuter_ticker", "GoogleFin_ticker", "B_Name",
                              "B_exchange", "B_description", "B_Address", "B_url",
                              "B_executives & board", "B_Sector",
                              "B_Industry", "B_Sub-industry"]
                writer = DictWriter(f, fieldnames=output_fieldnames)
                writer.writeheader()
                for ticker in bloomberg_crawler_logs:
                    bloomberg_company = ticker.get("data")
                    output_row = {
                        "Name": bloomberg_company.get('Name'),
                        "Exchange" : bloomberg_company.get("Exchange"),
                        "root_ticker" : bloomberg_company.get('root_ticker'),
                        "Bloomberg_ticker" : bloomberg_company.get('Bloomberg_ticker'),
                        "Reuter_ticker" : None,
                        "GoogleFin_ticker" : None,
                        "B_Name" : bloomberg_company.get("B_Name"),
                        "B_exchange" : bloomberg_company.get("B_exchange"),
                        "B_description" : bloomberg_company.get("B_description"),
                        "B_Address" : bloomberg_company.get("B_Address"),
                        "B_url" : bloomberg_company.get("B_url"),
                        "B_executives & board" : bloomberg_company.get("B_executives"),
                        "B_Sector" : bloomberg_company.get("B_Sector"),
                        "B_Industry" : bloomberg_company.get("B_Industry"),
                        "B_Sub-industry" : bloomberg_company.get("B_Sub_industry")
                    }
                    writer.writerow(output_row)
        except:
            logging.error('No documents found.')
            raise
