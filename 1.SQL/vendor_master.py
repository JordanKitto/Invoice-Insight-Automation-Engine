import pandas as pd
import oracledb  # Oracle database driver
import sys       # System-specific functions and parameters
import csv       # CSV file reading and writing
import time      # Time measurement for performance logging
import os        # Operating system interfaces (file path checks)

# Credential Configuration settings
hostname = 'oradsssbprod.health.qld.gov.au'
port = 2496
service_name = 'FINRPT.DB.HEALTH.QLD.GOV.AU'
user = 'FIN_AP_OPS'
password = 'F!N_Ap0ps2024'

# Table Configuration settings
table_transactionMaster = 'DSS.VIM_1HEAD_2HEAD_VW'
table_statusText = 'DSS.VIM_STG_T101T_VW'
table_layoutID = 'DSS.VIM_OTX_PF01_T_1REG_AP_VW'
table_vendorMaster_a = 'DSS.FS4_STG_LFA1_VW'
table_vendorMaster_b = 'DSS.FS4_STG_LFB1_VW'
table_paymentTerms = 'DSS.FS4_STG_T052U_VW'
table_zopp_vendor = 'DSS.VIM_STF_BUT0ID_V'
table_bcWorkplace_Error = 'DSS.VIM_OTX_PF01_T_1REG_AP_VW'
table_acdoca = 'DSS.FS4_ACDOCA_VW'




