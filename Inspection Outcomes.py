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
ReportName= 'Inspection Outcomes v1.0'
RunDate = datetime.now()

Insp_col = ['InspectionID',
            'InspectionStatus',
            'B1_ALT_ID',
            'GA_FNAME',
            'GA_LNAME'
            ]

# Set Query Variables
# list of possible fields
# Pos_Insp_col = [,
#            'InspectionID',
#            'NextInspectionDate',
#            'InspectionCompleteDate',
#            'G6_ACT_GRP',
#            'G6_ACT_TYP',
#            'G6_ACT_DES',
#            'G6_DOC_DES',
#            'INSP_GROUP',
#            'INSP_GROUP_NAME',
#            'G6_STATUS',
#            'InspectionStatus',
#            'StatusDate',
#            'R3_DEPTNAME',
#            'GA_FNAME',
#            'GA_MNAME',
#            'GA_LNAME',
#            'INSP_SEQ_NBR',
#            'GA_USERID',
#            'INSP_RESULT_TYPE',
#            'RESCHEDULE_FROM_SEQ',
#            'REC_DATE',
#            'REC_FUL_NAM',
#            'REC_STATUS',
#            'Subtype']


# Load data

# Load base lists
RSCInsp = pd.read_csv(r'\\fileservices02\ManagedShares\Data\Implementation Support\Python Scripts\StaticData\RSCCampaignInspections.csv')
SegInsp = pd.read_csv(r'\\fileservices02\ManagedShares\Data\Implementation Support\Python Scripts\StaticData\SegmentationCampaignInspections.csv')
RSCInsp_List = RSCInsp['RSC - Inspection ID'].values.tolist()
SegInsp_List = SegInsp['Segmentation InspectionID'].values.tolist()


# Query SQL tables
RSCOutcomes = pdsql.mssql.rd_sql('SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = Insp_col,
                   where_in = {'InspectionID': RSCInsp_List})

SegOutcomes = pdsql.mssql.rd_sql('SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = Insp_col,
                   where_in = {'InspectionID': SegInsp_List})

# Find Missing Inspections
RSCMissing_Count = RSCInsp.shape[0]-RSCOutcomes.shape[0]
SegMissing_Count = SegInsp.shape[0]-SegOutcomes.shape[0]

RSCOutcomes = pd.merge(RSCInsp, RSCOutcomes, 
                        left_on='RSC - Inspection ID',
                        right_on='InspectionID',
                        how='outer')
RSCOutcomes['InspectionStatus'] = RSCOutcomes['InspectionStatus'].fillna('Missing Inspection')
#RSCMissingInsp = RSCOutcomes[RSCOutcomes['InspectionID'].isnull()]
#RSCMissingInsp_List = RSCMissingInsp['RSC - Inspection ID'].values.tolist()

SegOutcomes = pd.merge(SegInsp, SegOutcomes, 
                        left_on='Segmentation InspectionID',
                        right_on='InspectionID',
                        how='outer')
SegOutcomes['InspectionStatus'] = SegOutcomes['InspectionStatus'].fillna('Missing Inspection')



# Create aggragate info
RSCCounts = pd.DataFrame(
        RSCOutcomes.groupby(['InspectionStatus'])['RSC - Inspection ID'].count())

SegCounts = pd.DataFrame(
        SegOutcomes.groupby(['InspectionStatus'])['Segmentation InspectionID'].count())

RSCCounts.columns = ['Inspection Count']
SegCounts.columns = ['Inspection Count']


# Print email
print(
      'Hi Carly,\n\n Below are the inspection outcomes from the list you provided as of ',
      datetime.strftime(datetime.now() - timedelta(days =1), '%d-%m-%Y'),
      '\nThere are', RSCMissing_Count, 'RSC inspections and',
      SegMissing_Count, 'Segmentation inspections missing.',
      '\n\nRegionally Significant Consent Inspection Outcomes\n\n',RSCCounts,
      '\n\n\nWater Use Segmentation Inspection Outcomes\n\n',SegCounts,
      '\n\n* Note: This report was created at ',
      RunDate.strftime("%H:%M %Y-%m-%d"), ' using ', ReportName,
      '\n\nCheers,\nKatie\n'
      )





















