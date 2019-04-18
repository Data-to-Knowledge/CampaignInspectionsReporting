# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 08:59:22 2019

@author: KatieSi

Weekly Inspection Outcomes
Due Monday 10am until June 30th.

Run at 8:30 am Mondays
Output tables e-mailed to Carly Waddleton
"""
# Import packages
import pandas as pd
import pdsql
from datetime import datetime, timedelta

#Set Variables
ReportName= 'Scheduled RSC Inspections'
RunDate = datetime.now()

Insp_col = ['InspectionID',
            'InspectionStatus',
            'B1_ALT_ID',
            'GA_FNAME',
            'GA_LNAME'
            ]

InspColumnNames = {
        'B1_ALT_ID' : 'Consent',
        'GA_FNAME' : 'OfficerAssigned'
        }

# InspColumnDrop = ['GA_LNAME']

Holder_col = [
        'B1_ALT_ID',
        'HolderAddressFullName',
        'MonOfficerDepartment'
        ]
HolderColumnNames = {
        'B1_ALT_ID' : 'Consent',
        'HolderAddressFullName' : 'Consent Holder'
        }


# Load base lists
RSCInsp = pd.read_csv(r'\\fileservices02\ManagedShares\Data\Implementation Support\Python Scripts\StaticData\RSCCampaignInspections.csv')
RSCInsp_List = RSCInsp['RSC - Inspection ID'].values.tolist()


# Query SQL Inspection table
RSCOutcomes = pdsql.mssql.rd_sql('SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = Insp_col,
                   where_in = {'InspectionID': RSCInsp_List})



# Creat Scheduled Inspection list

RSCScheduled = RSCOutcomes[RSCOutcomes.InspectionStatus=='Scheduled']
RSCSchedInsp_List = RSCScheduled['B1_ALT_ID'].values.tolist()



# Query SQL permit table

ConsentHolder = pdsql.mssql.rd_sql('SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'F_ACC_Permit',
                   col_names = Holder_col,
                   where_in = {'B1_ALT_ID': RSCSchedInsp_List})



# Format Consent holder table

ConsentHolder.rename(columns=HolderColumnNames, inplace=True)

#ConsentHolder.to_csv('Scheduled RSC Inspections.csv')
