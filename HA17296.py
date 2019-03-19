import ibm_db
import datetime
import os
import mskcc
import csv
import xlsxwriter
import pypyodbc


###########################
#       CONNECTION        #
###########################

input_file_1 = '../properties.txt'
f_in = open(input_file_1, 'r')
properties_dict = {}
for line in f_in:
    properties_dict[line.partition('=')[0]] = line.partition('=')[2].strip()
f_in.close()

connection_idb = ibm_db.connect('DATABASE=DB2P_MF;'
                     'HOSTNAME=ibm3270;'
                     'PORT=3021;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["idb_service_uid1"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["idb_service_pwd1"]).decode("latin-1")), '', '')

connection_darwin = ibm_db.connect('DATABASE=DVPDB01;'
                     'HOSTNAME=pidvudb1di1vipdb01;'
                     'PORT=51013;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["darwin_uid"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["darwin_pwd"]).decode("latin-1")), '', '')

connection_sql_server = pypyodbc.connect("Driver={{SQL Server}};Server={};Database={};Uid={};Pwd={};".format(
                    "PS23A,61692",
                    "DEDGPDLR2D2",
                    properties_dict["sqlserver_ps23a_uid"],
                    mskcc.decrypt(properties_dict["sqlserver_ps23a_pwd"]).decode("latin-1")
                    )
                )

###########################
#         DECLARE         #
###########################

now_raw = datetime.datetime.now()
now = now_raw.strftime('%Y%m%d-%H%M%S')
today = now_raw.strftime('%Y-%m-%d')
today_mm_dd_yyyy = now_raw.strftime('%m/%d/%Y')
dataline_report_number = os.path.basename(__file__).replace(".py", "")

# file vars
input_file_1 = r'\\vpenshin\HinShared\TT Reports\TTPendDC.csv'
#input_file_1 = r'\\vpenshin\HinShared\TT Reports\TTPendDC.2019-03-05.csv'
output_file_1 = 'Pending Discharges - ({}) {}.xlsx'.format(dataline_report_number, now)

# Excel vars
workbook = xlsxwriter.Workbook(output_file_1)
worksheet_results = workbook.add_worksheet('TT Pending DC Today - Attending')
col_widths = []


# create table DADM.HA17296
#     (HA17296_HOSP_SVC VARCHAR(100),
#      HA17296_HOSP_SVC_DESC CHAR(254),
#      HA17296_HOME_ABBR VARCHAR(10),
#      HA17296_FULLNAME VARCHAR(254),
#      HA17296_MRN CHAR(12),
#      HA17296_VIS_ADM_NUM CHAR(10),
#      HA17296_EXP_DSCH TIMESTAMP,
#      HA17296_ATTENDING_NAME VARCHAR(254),
#      HA17296_ATTENDING_ID CHAR(12),
#      HA17296_ATTENDING_EMAIL VARCHAR(254),
#      HA17296_LOCATION CHAR(12));

###########################
#        FUNCTIONS        #
###########################

def output_excel_column_headers_list(worksheet, in_list, row, col_start):
  fmt = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1'})
  d=col_start
  #col_widths = [0 for n in range(0, len(in_list))]
  for n in range(0, len(in_list)):
    col_widths.append(len(in_list[n])+3)
    worksheet.write(row, d+n, in_list[n], fmt)
    worksheet.set_column(d+n, d+n, col_widths[n])

def output_excel_list_width_calc(worksheet, in_list, row):
  for col, cell in enumerate(in_list):
    #print("col_widths[col]: {}, len(cell): {}".format(col_widths[col], len(cell)))
    if isinstance(cell, datetime.date):
      worksheet.write(row, col, cell.strftime('%Y-%m-%d'))
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    if isinstance(cell, datetime.datetime):
      worksheet.write(row, col, cell.strftime('%Y-%m-%d %I:%M %p'))
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    elif isinstance(cell, str):
      worksheet.write(row, col, cell.strip())
      if len(cell.strip()) > col_widths[col]:
        col_widths[col] = len(cell.strip())
    elif isinstance(cell, int):
      worksheet.write(row, col, cell)
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    else:
      worksheet.write(row, col, cell)
      if len(cell) > col_widths[col]:
        col_widths[col] = len(cell)
  for col, width in enumerate(col_widths):
    worksheet.set_column(col, col, width+3)
  return 0

def row_to_dict(row_raw, columns):
  row = {}
  x = 0
  for col in columns:
      row[col] = row_raw[x]
      x += 1
  return row

def get_recipients(dataline_report_number):
  recipient_list = []
  SQL = """
    select recipient + '@mskcc.org' recipient
    from dbo.scheduler 
    join dbo.scheduler_recipients on scheduler_id=id
    where enabled=1 and project_code = '{}'
  """.format(dataline_report_number)

  cursor = connection_sql_server.cursor()
  cursor.execute(SQL)

  row = {}
  row_raw = cursor.fetchone()
  while row_raw is not None:
      columns = [column[0] for column in cursor.description]
      row = row_to_dict(row_raw, columns)

      recipient_list.append(row["recipient"])
      row_raw = cursor.fetchone()

  cursor.close()
  return recipient_list

###########################
#          MAIN           #
###########################

if __name__ == "__main__":

  attending_to_html_dict = {}

  column_names = ['Service', 'Unit #', 'Bed', 'Patient Name', 'MRN', 'Attending', 'Expected Discharge Date and Time']

  output_excel_column_headers_list(worksheet_results, column_names, 0, 0)

  with open(input_file_1, 'r') as f:
    lines = f.readlines()

  # ExpectedDischargeDateTime_Value,HomeUnitAbbreviation_Value,FullName_Value,MedicalRecordNumber_Value,VisitNumber_Value,AttendingPhysicianFullName_Value,AttendingPhysicianADTID_Value,HomeLocationAbbreviation_Value
  for line in csv.reader(lines[4:], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    if len(line) == 8:
      ExpectedDischargeDateTime_Value = line[0].strip()
      HomeUnitAbbreviation_Value = line[1].strip()
      FullName_Value = line[2].strip()
      MedicalRecordNumber_Value = line[3].strip()
      VisitNumber_Value = line[4].strip()
      print(ExpectedDischargeDateTime_Value)
      date_str, time_str, am_pm_str = ExpectedDischargeDateTime_Value.split(' ')
      discharge_dt = datetime.datetime.strptime("{} {}:{} {}".format(date_str, time_str.split(':')[0].zfill(2), time_str.split(':')[1], am_pm_str), "%m/%d/%Y %I:%M %p")
      #print("{} {}:{} {}".format(date_str, time_str.split(':')[0].zfill(2), time_str.split(':')[1], am_pm_str))
      discharge_dt_str = datetime.datetime.strftime(discharge_dt, "%Y-%m-%d %H:%M:%S")

      AttendingPhysicianFullName_Value = line[5].strip()
      AttendingPhysicianADTID_Value = line[6].strip().zfill(6)
      HomeLocationAbbreviation_Value = line[7].strip()

      sql_string = """

            select TRIM(VIS_ATN_DR_NO) VIS_ATN_DR_NO, TRIM(ATN_DR_NAME) ATN_DR_NAME
            from idb.visit_V
            join idb.visit_xref on vis_adm_num = vx_adm_num
            where vx_xref_visit_num = '{}'

      """.format(VisitNumber_Value)

      stmt = ibm_db.prepare(connection_darwin, sql_string)

      print(sql_string)

      ibm_db.execute(stmt)

      db_dict = ibm_db.fetch_both(stmt)

      if db_dict != False:
        AttendingPhysicianADTID_Value = db_dict["VIS_ATN_DR_NO"]        
        AttendingPhysicianFullName_Value = db_dict["ATN_DR_NAME"]        

      sql_string = """

        SELECT cre_res_id, lower(trim(cre_email_addr)) email
        from idb.cl_resource
        where CRE_RES_ID = '{}'

      """.format(AttendingPhysicianADTID_Value)

      stmt = ibm_db.prepare(connection_idb, sql_string)

      print(sql_string)

      ibm_db.execute(stmt)

      db_dict = ibm_db.fetch_both(stmt)

      attending_email = ''
      if db_dict != False:
        attending_email = db_dict["EMAIL"]

      sql_string = """

        select 1 rnk, vis_adm_num, trim(vis_hosp_svc) HOSP_SVC, trim(hosp_svc_desc) HOSP_SVC_DESC
        from idb.visit_V
        join idb.visit_xref on vis_adm_num = vx_adm_num
        where vx_xref_visit_num = '{}'

        union

        select ROW_NUMBER() OVER (PARTITION BY VIS_MRN ORDER BY VIS_DTE DESC)+1 rnk, vis_adm_num, trim(vis_hosp_svc) HOSP_SVC, trim(hosp_svc_desc) HOSP_SVC_DESC
        from idb.visit_V
        where vis_mrn = '{}' and vis_dte is not null and replace(replace(coalesce(VIS_BED, ' '), ' ', ''), '_', '') = '{}'

        order by 1

      """.format(VisitNumber_Value, MedicalRecordNumber_Value, HomeLocationAbbreviation_Value)

      stmt = ibm_db.prepare(connection_darwin, sql_string)

      print(sql_string)

      ibm_db.execute(stmt)

      db_dict = ibm_db.fetch_both(stmt)

      hosp_svc = ''
      hosp_svc_desc = ''
      if db_dict != False:
        hosp_svc = db_dict["HOSP_SVC"]
        hosp_svc_desc = db_dict["HOSP_SVC_DESC"]
      else:
        sql_string = """

          select distinct vis_adm_num, trim(vis_hosp_svc) HOSP_SVC, trim(hosp_svc_desc) HOSP_SVC_DESC
          from idb.visit_V
          --join idb.patient_keeper on vis_mrn=pk_mrn and vis_adm_dte=pk_adm_dte
          join idb.visit_xref on vis_adm_num = vx_adm_num
          where vx_xref_visit_num = '{}'

        """.format(VisitNumber_Value)

        stmt = ibm_db.prepare(connection_idb, sql_string)

        print(sql_string)

        ibm_db.execute(stmt)

        db_dict = ibm_db.fetch_both(stmt)

        if db_dict != False:
          hosp_svc = db_dict["HOSP_SVC"]
          hosp_svc_desc = db_dict["HOSP_SVC_DESC"]

      a, b, c, d, e, f, g = column_names
      if attending_email and attending_email not in attending_to_html_dict:
        attending_to_html_dict[attending_email] = """
          <table style='margin:auto;width:85%'>
            <tr style='background:#528AE7;font-family:Tahoma;color:white;font-size: 11.0pt;'><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
        """.format(a, b, c, d, e, f, g)

      if attending_email:
        attending_to_html_dict[attending_email] += """
            <tr style='background:white;font-family:Tahoma;color:black;font-size: 11.0pt;border:.5pt solid silver;'><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
        """.format(hosp_svc_desc, HomeUnitAbbreviation_Value, HomeLocationAbbreviation_Value, FullName_Value, MedicalRecordNumber_Value, AttendingPhysicianFullName_Value, ExpectedDischargeDateTime_Value)

      sql_string = """

        INSERT INTO DADM.HA17296 (HA17296_HOSP_SVC, HA17296_HOSP_SVC_DESC, HA17296_HOME_ABBR, HA17296_FULLNAME, HA17296_MRN, 
                                  HA17296_VIS_ADM_NUM, HA17296_EXP_DSCH, HA17296_ATTENDING_NAME, HA17296_ATTENDING_ID, 
                                  HA17296_ATTENDING_EMAIL, HA17296_LOCATION)
        SELECT '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}' from sysibm.sysdummy1
        MINUS
        SELECT * FROM DADM.HA17296

      """.format(hosp_svc, hosp_svc_desc, HomeUnitAbbreviation_Value, FullName_Value.replace("'", "''"), MedicalRecordNumber_Value, 
        VisitNumber_Value, discharge_dt_str, AttendingPhysicianFullName_Value.replace("'", "''"), AttendingPhysicianADTID_Value, 
        attending_email, HomeLocationAbbreviation_Value)

      print(sql_string)

      stmt = ibm_db.prepare(connection_idb, sql_string)
      ibm_db.execute(stmt)

  sql_string = """SELECT HA17296_HOSP_SVC_DESC, HA17296_HOME_ABBR, HA17296_LOCATION, HA17296_FULLNAME, HA17296_MRN, HA17296_ATTENDING_NAME, HA17296_EXP_DSCH 
                  FROM DADM.HA17296 
                  WHERE DATE(HA17296_EXP_DSCH) = current date --'2018-12-14' 
                  ORDER BY HA17296_HOSP_SVC_DESC, HA17296_HOME_ABBR, HA17296_ATTENDING_NAME"""

  print(sql_string)

  stmt = ibm_db.prepare(connection_idb, sql_string)
  ibm_db.execute(stmt)

  db_tup = ibm_db.fetch_tuple(stmt)
  row = 1
  while db_tup != False:
    #lst = [db_dict[key] for key in db_dict.keys()]
    output_excel_list_width_calc(worksheet_results, db_tup, row)
    row+=1
    db_tup = ibm_db.fetch_tuple(stmt)

  workbook.close()

  for attending_email in attending_to_html_dict:
    email_body = """
        <style>
        table {
          border-collapse: collapse;
        }

        table, th, td {
          border: 1px solid black;
        }
        </style>
    """

    #email_body += "This would be sent to: {}\n".format(attending_email) + attending_to_html_dict[attending_email].replace("'", '"').replace('\n', '').replace('\r', '') + "</tr></table>"
    email_body += attending_to_html_dict[attending_email].replace("'", '"').replace('\n', '').replace('\r', '') + "</tr></table>"
    #attending_email = "singerm@mskcc.org"
    
    sql_string = """

      select DV.SENDJAVAXMAIL('Data/Information Systems <data@mskcc.org>','singerm@mskcc.org;{}','','','Pending Discharges ({}) - {}','{}','text/html;charset=utf-8')
      from SYSIBM.SYSDUMMY1

    """.format(attending_email, dataline_report_number, today, email_body)
    print(sql_string)
    stmt = ibm_db.prepare(connection_darwin, sql_string)
    if attending_email not in ('kinghamt@mskcc.org', 'dangelim@mskcc.org'): # everyone should receive this according to Narges - there is no unsubscribe
      ibm_db.execute(stmt)

  attachments = []
  with open(output_file_1, 'rb') as f:
      content = f.read()
  attachments.append((output_file_1, content))

  #attachments = [output_file_1]
  
  email_subject = "Hospital Discharges Pending for Today {}".format(today_mm_dd_yyyy)
  email_body = """Hi,

Please find attached the list of patients anticipated for hospital discharge today. Please prioritize these patients this morning by entering all Discharge Orders as early as possible to expedite hospital discharge.  

For Nursing, Rehab, Pharmacy, Case Management and all teams, please prioritize the work as early in the day as possible for patients on this list, in order to facilitate early discharges and allow for efficient patient transport and bed turn-over.

Due to our collective efforts, patients are reporting an improved experience at MSK, with greater coordination and efficiency in our on-going improvements with the discharge process.

Thank you"""
  #email_recipients = get_recipients(dataline_report_number)
  email_recipients = ['singerm@mskcc.org','zzPDL_NUR_DCCoordinators@mskcc.org','zzPDL_Rehab_MainCampus_Management@mskcc.org','zzPDL_HAD_Discharge_Admitting@mskcc.org','zzPDL_HAD_Discharge_Leadership@mskcc.org','zzPDL_HAD_Discharge_Rehab@mskcc.org','zzPDL_HAD_Discharge_Case_Management@mskcc.org','zzPDL_PHA_PharmacyClinicalGroup@mskcc.org','zzPDL_PHA_PharmacyResidents@mskcc.org','zzPDL_PHA_MainCampus_OPD@mskcc.org','zzPDL_FSV_Room_Service_Management_Team@mskcc.org','zzPDL_NUR_Charge_Nurses@mskcc.org','zzPDL_NUR_NurseLeaders_CriticalCare_Pediatrics@mskcc.org','zzPDL_PhysicianAssistants_ALL@mskcc.org','zzPDL_SUR_ClinicalFellows@mskcc.org','zzPDL_NUR_Nursing_Support_Services_Management@mskcc.org','zzPDL_NUR_NurseLeaders_Acute_Care@mskcc.org','PDL_NUR_NursePractitioners@mskcc.org','unitassts@mskcc.org','zzPDL_GeneralServices_EnvironmentalServicesSupervisor@mskcc.org']
  #email_recipients = ['singerm@mskcc.org']
  
  mskcc.send_email(email_subject, email_body, email_recipients, attachments=attachments)
  #mskcc.send_mail("Data/Information Systems <data@mskcc.org>", ";".join(email_recipients), email_subject, email_body, attachments, html=False)

  os.remove(output_file_1)
  os.rename(input_file_1, "{}.{}.csv".format(input_file_1.replace(".csv", ""), today))
