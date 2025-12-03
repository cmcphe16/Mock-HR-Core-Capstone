import pandas as pd
import pyodbc 
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np
import random


#extract our "fact" flat files first
factattendance = pd.read_csv(
    r"C:\Users\Cameron McPherson\Downloads\ETL\fact_attend.csv",
    parse_dates=['date','clock_in_time','clock_out_time']
)
factperformance = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\DimPerformance.csv")
#factPTO = pd.read_csv(r"C:\Users\jvelas6\OneDrive - Emory\Documents\Grad School Assignments\IT Capstone\HR_fixed_bundle\DimPTO.csv", parse_dates=['date'])
factrequests = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\fact_request.csv", parse_dates=['request_date', 'start_date', 'end_date'])
factsurvey = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\fact_survey.csv", parse_dates=['survey_date'] )
facttraining = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\fact_train.csv")
#dimensiontables
employee = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\dim_employee.csv")
manager = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\dim_manager.csv")
calendar = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\dim_calendar.csv", parse_dates=['date'])
department = pd.read_csv(r"C:\Users\Cameron McPherson\Downloads\ETL\dim_department.csv")



#creating surrogatekeys/dropping duplicates. it should be only unique values in our dimension tables.
#employeekey/
employee = employee.drop_duplicates(subset=["employee_id"]).reset_index(drop=True)
employee.insert(0, "EmployeeKey", range(1, len(employee) + 1))
#datekey
calendar["DateKey"] = calendar["date"].dt.strftime("%Y%m%d").astype(int)
calendar = calendar.drop_duplicates(subset=["DateKey"]).reset_index(drop=True)

#DepartmentKey
department = department.drop_duplicates(subset=["department"]).reset_index(drop=True)
department.insert(0, "DepartmentKey", range(1, len(department) + 1))
#  ManagerKey/drop duplicates.
manager = manager.drop_duplicates(subset=["manager_id"]).reset_index(drop=True)
manager.insert(0, "ManagerKey", range(1, len(manager) + 1))


#add unknown
unknown_employee = pd.DataFrame([{
    "EmployeeKey": 0,
    "employee_id": 0,
    "name": "Unknown",
    "job_title": "Unknown",
    "department": "Unknown",
    "office_location": "Unknown",
    "hire_date": pd.NaT,
    "salary_band": 0,
    "birth_month": 0,
    "gender": "Unknown",
    "education_level": "Unknown",
    "remote_eligibility": pd.NaT,
    "preferred_work_mode": "Unknown",
    "department_key": 0,
    "tenure_years": 0,
    "ethnicity": "Unknown"
}])
employee = pd.concat([unknown_employee, employee], ignore_index=True)
#clean up.

#going to use today's date to make the tenure_years a bit more accurate as if it is in real time.

today = pd.Timestamp.today()
employee["tenure_years"] = (today - pd.to_datetime(employee['hire_date'])).dt.days / 365.25
employee["tenure_years"] = employee["tenure_years"].round(2)

#generate birth date because most hr companies would have your birth date on file.
def generate_birth_date():
    years_ago = random.randint(22, 70)
    days_offset = random.randint(0, 364)
    birth_date = datetime.now() - timedelta(days=years_ago*365 + days_offset)
    return birth_date.strftime('%Y-%m-%d')

employee['birth_date'] = None
for idx in range(1, len(employee)):  # Start from 1 to skip Unknown
    employee.loc[idx, 'birth_date'] = generate_birth_date()

employee['age'] = employee['birth_date'].apply(
    lambda x: (datetime.now() - pd.to_datetime(x)).days // 365 if pd.notna(x) else None
)


#let's generate seperation date and reason also will be adding an age column.
reasons = ['New Opportunity', 'Work Environment','Changing Careers','Retired']
retirement_age = 65
turnover_rate = 0.25

has_left = np.random.choice([True,False], size=len(employee), p=[turnover_rate, 1-turnover_rate])

employee['Separation_Date'] = None
employee['Separation_Reason'] = None


for idx in employee[has_left].index:
    # Skip the Unknown employee
    if idx == 0:
        continue
    
    hire_date = pd.to_datetime(employee.loc[idx, 'hire_date'])
    days_since_hire = (datetime.now() - hire_date).days
    current_age = employee.loc[idx, 'age']
    
    if days_since_hire > 65:
        days_employed = random.randint(30, days_since_hire)
        last_date = hire_date + timedelta(days=days_employed)
        
        # Calculate age at time of leaving
        age_at_leaving = current_age - ((datetime.now() - last_date).days // 365)
        
        employee.loc[idx, 'Separation_Date'] = last_date.strftime('%Y-%m-%d')
        
        # Logic for separation reason based on age
        if age_at_leaving >= retirement_age:
            # High probability of retirement for older employees
            employee.loc[idx, 'Separation_Reason'] = random.choices(
                reasons,
                weights=[0.1, 0.05, 0.05, 0.8]  # 80% chance Retired
            )[0]
        elif age_at_leaving >= 60:
            # Moderate probability of retirement for near-retirement age
            employee.loc[idx, 'Separation_Reason'] = random.choices(
                reasons,
                weights=[0.3, 0.15, 0.15, 0.4]  # 40% chance Retired
            )[0]
        else:
            # Young employees rarely retire
            valid_reasons = ['New Opportunity', 'Work Environment', 'Changing Careers']
            employee.loc[idx, 'Separation_Reason'] = random.choice(valid_reasons)

employee = employee.drop('birth_month', axis=1)

employee['Separation_Date'] = pd.to_datetime(employee['Separation_Date'], errors='coerce')
employee['SeparationDateKey'] = employee['Separation_Date'].dt.strftime('%Y%m%d').astype('Int64')

unknown_manager = pd.DataFrame([{
    "ManagerKey": 0,
    "manager_id": 0,
    "manager_name": "Unknown",
    "manager_department": "Unknown"
}])
manager = pd.concat([unknown_manager, manager], ignore_index=True)

unknown_calendar = pd.DataFrame([{
    "DateKey": 0,
    "date": pd.NaT,
    "year": 0,
    "quarter": 0,
    "month": 0,
    "day": 0,
    "weekday": 0,
    "is_weekend": 0,
    "week_of_year": 0,
    "month_name": "UNK",
    "dow_name": "UNK"
}])
calendar = pd.concat([unknown_calendar, calendar], ignore_index=True)

calendar["is_weekend"] = calendar["is_weekend"].astype(int)
calendar["week_of_year"] = calendar["week_of_year"].fillna(0).astype(int)

unknown_department = pd.DataFrame([{
    "DepartmentKey": 0,
    "department": "Unknown", 
    "department_key": 0
    }])

department = pd.concat([unknown_department, department], ignore_index=True)

# 
employee['hire_date'] = pd.to_datetime(employee['hire_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# 
calendar['date'] = pd.to_datetime(calendar['date'], errors='coerce').dt.strftime('%Y-%m-%d')

# 
if 'date' in factattendance.columns:
    factattendance['date'] = pd.to_datetime(factattendance['date'], errors='coerce').dt.strftime('%Y-%m-%d')
if 'request_date' in factrequests.columns:
    factrequests['request_date'] = pd.to_datetime(factrequests['request_date'], errors='coerce').dt.strftime('%Y-%m-%d')
if 'start_date' in factrequests.columns:
    factrequests['start_date'] = pd.to_datetime(factrequests['start_date'], errors='coerce').dt.strftime('%Y-%m-%d')
if 'end_date' in factrequests.columns:
    factrequests['end_date'] = pd.to_datetime(factrequests['end_date'], errors='coerce').dt.strftime('%Y-%m-%d')
if 'survey_date' in factsurvey.columns:
    factsurvey['survey_date'] = pd.to_datetime(factsurvey['survey_date'], errors='coerce').dt.strftime('%Y-%m-%d')
#if 'date' in factPTO.columns:
  #  factPTO['date'] = pd.to_datetime(factPTO['date'], errors='coerce').dt.strftime('%Y-%m-%d')


#fact attendance table
factattendance['HoursWorked'] = (factattendance['clock_out_time'] - factattendance['clock_in_time']).dt.total_seconds() / 3600

factattendance['HoursWorked'] = factattendance['HoursWorked'].round(1)

factattendance = factattendance.merge(employee[['EmployeeKey', 'employee_id']], on='employee_id', how='left')

factattendance = factattendance.merge(calendar[['DateKey', 'date']], on='date', how='left')

fact_attendance = factattendance[['EmployeeKey', 'DateKey', 'HoursWorked', 'remote_flag']]
fact_attendance.insert(0, "FactAttendanceID", range(1, len(fact_attendance) + 1))

#build fact_pto table
#removing anything related to factpto.
#factPTO = factPTO.merge(employee[['EmployeeKey', 'employee_id']], on='employee_id', how='left')
#factPTO = factPTO.merge(calendar[['DateKey', 'date']], on='date', how='left')

#fact_pto = factPTO[[
   # 'EmployeeKey',
    #'DateKey', 
    #'hours',
    #'pto_type'
#]]
#adding like a factpto id
#fact_pto.insert(0, "FactPTOID", range(1, len(fact_pto)+1))

#building factrequest table
factrequests = factrequests.merge(employee[['EmployeeKey','employee_id']], on='employee_id', how='left')

factrequests = factrequests.merge(calendar[['DateKey', 'date']], left_on = 'request_date', right_on='date', how='left').rename(columns={"DateKey":"RequestDateKey"}).drop(columns='date')
factrequests = factrequests.merge(calendar[['DateKey', 'date']], left_on = 'start_date', right_on='date', how='left').rename(columns={"DateKey":"StartDateKey"}).drop(columns='date')
factrequests = factrequests.merge(calendar[['DateKey', 'date']], left_on = 'end_date', right_on='date', how='left').rename(columns={"DateKey":"EndDateKey"}).drop(columns='date')


today_key = int(pd.Timestamp.today().strftime('%Y%m%d'))

factrequests = factrequests[factrequests['RequestDateKey'] <= today_key]

mask_jury_travel = (
    factrequests['pto_type'].str.lower().eq('jury duty') &
    factrequests['reason'].str.lower().str.contains('travel', na=False)
)
factrequests.loc[mask_jury_travel, 'reason'] = 'Court Mandate'

factrequests.loc[factrequests['reason'] == 'Auto-generated to satisfy constraints', 'reason'] = (
    factrequests['pto_type'].map({
        'Vacation': 'Personal Time Off',
        'Sick Leave': 'Medical Reason',
        'Jury Duty': 'Court Mandate'
    }).fillna('General PTO Request')
)

factrequests['RequestDateKey'] = factrequests['RequestDateKey'].astype('Int64')
factrequests['StartDateKey'] = factrequests['StartDateKey'].astype('Int64')
factrequests['EndDateKey'] = factrequests['EndDateKey'].astype('Int64')

factrequests['approved'] = factrequests['approved'].astype(bool)

factrequests['days_diff'] = (
    pd.to_datetime(factrequests['EndDateKey'], format='%Y%m%d', errors='coerce') -
    pd.to_datetime(factrequests['StartDateKey'], format='%Y%m%d', errors='coerce')
).dt.days

factrequests['hours_taken'] = factrequests.apply(
    lambda row: 0
    if not row['approved']
    else (8 if pd.isna(row['days_diff']) or row['days_diff'] <= 0 else (row['days_diff'] + 1) * 8),
    axis=1
)


factrequests = factrequests.drop(columns=['days_diff'])

factrequests['approved'] = factrequests['approved'].astype(int)

fact_request = factrequests[[
    'EmployeeKey',
    'RequestDateKey',
    'StartDateKey',
    'EndDateKey',
    'pto_type',
    'approved',
    'reason',
    'hours_taken'
]]


fact_request.insert(0, "FactRequestID", range(1, len(fact_request)+1))

#building factsurvey
factsurvey = factsurvey.merge(employee[['EmployeeKey','employee_id']], on='employee_id', how='left')

factsurvey = factsurvey.merge(calendar[['DateKey','date']], left_on='survey_date', right_on='date', how='left')

fact_survey = factsurvey[[
    'EmployeeKey',
    'DateKey',
    'satisfaction_score',
    'engagement_index',
    'manager_effectiveness',
    'enps',
    'comments'
]]
fact_survey.insert(0,'FactSurveyID', range(1, len(fact_survey)+1))

#building facttraining
facttraining = facttraining.merge(employee[['EmployeeKey','employee_id']], on='employee_id', how='left')

dim_course = facttraining[["course_code","course_name"]].drop_duplicates().reset_index(drop=True)
dim_course.insert(0, "CourseKey", range(1, len(dim_course)+1))

unknown_course = pd.DataFrame([{
    "CourseKey": 0,
    "course_code": "UNK",
    "course_name": "Unknown"
}])

dim_course = pd.concat([unknown_course, dim_course], ignore_index=True)

facttraining = facttraining.merge(dim_course, on=["course_code","course_name"], how="left")
fact_training = facttraining[[
    "EmployeeKey",
    "CourseKey",
    "score"
]]

fact_training.insert(0, "FactTrainingID", range(1, len(fact_training)+1))
#these lines below are meant to delete any records that have dates post seperation date from dimemployee

fact_request = fact_request.merge(employee[['EmployeeKey','SeparationDateKey']], on='EmployeeKey', how='left')
fact_request = fact_request[
    (fact_request['SeparationDateKey'].isna()) |
    (fact_request['RequestDateKey'] <= fact_request['SeparationDateKey'])
].drop(columns=['SeparationDateKey'])

# FactAttendance
fact_attendance = fact_attendance.merge(employee[['EmployeeKey','SeparationDateKey']], on='EmployeeKey', how='left')
fact_attendance = fact_attendance[
    (fact_attendance['SeparationDateKey'].isna()) |
    (fact_attendance['DateKey'] <= fact_attendance['SeparationDateKey'])
].drop(columns=['SeparationDateKey'])

# FactSurvey
fact_survey = fact_survey.merge(employee[['EmployeeKey','SeparationDateKey']], on='EmployeeKey', how='left')
fact_survey = fact_survey[
    (fact_survey['SeparationDateKey'].isna()) |
    (fact_survey['DateKey'] <= fact_survey['SeparationDateKey'])
].drop(columns=['SeparationDateKey'])


if 'SeparationDateKey' in employee.columns:
    employee = employee.drop(columns=['SeparationDateKey'])




#trying to load into sql server.
#must find your own stuff.
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=(local);'
    'DATABASE=Capstone_HR;'
    'Trusted_Connection=yes;'
)

print("Connected successfully!")

#this may need to be changed. i believe you have to go to sql driver setttings.

engine = create_engine('mssql+pyodbc://(local)/Capstone_HR?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

fact_tables = ["FAttendance","FRequest", "FSurvey", "FTrain"]
dim_tables = ["DimEmployee", "DimDepartment", "DimManager","DimCalendar","DimCourse"]

with engine.begin() as conn:
    print("Disabling all foreign key constraints...")
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';"))

    print("Clearing fact tables...")
    for table in fact_tables:
        conn.execute(text(f"DELETE FROM {table};"))

    print("Clearing dimension tables...")
    for table in dim_tables:
        conn.execute(text(f"DELETE FROM {table};"))

    print("Re-enabling foreign key constraints...")
    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL';"))

print("All tables cleared successfully and constraints re-enabled.")

with engine.begin() as conn:
    employee.to_sql('DimEmployee', con=conn, if_exists='append', index=False)
    department.to_sql('DimDepartment', con=conn, if_exists='append', index=False)
    manager.to_sql('DimManager', con=conn, if_exists='append', index=False)
    calendar.to_sql('DimCalendar', con=conn, if_exists='append', index=False)
    dim_course.to_sql('DimCourse', con=conn, if_exists='append', index=False)

print("All Dimension Tables have been loaded.")


#fact tables to drop and during test runs. 


with engine.begin() as conn2:
    fact_attendance.to_sql('FAttendance', con=conn2, if_exists='append', index=False)
    #fact_pto.to_sql('FPTO', con=conn2, if_exists='append', index=False)
    fact_survey.to_sql('FSurvey', con=conn2, if_exists='append', index=False)
    fact_training.to_sql('FTrain', con=conn2, if_exists='append', index=False)
    fact_request.to_sql('FRequest', con=conn2, if_exists='append', index=False)

print("Fact Tables have been created.")

#outputting final results to view if you would like. they're commented out for now.
#fact_attendance.to_csv(r"{filepath}", index=False)
#fact_pto.to_csv(r"{filepath}", index=False)
#fact_survey.to_csv(r"{filepath}", index=False)
#fact_training.to_csv(r"{filepath}", index=False)
#fact_request.to_csv(r"{filepath}", index=False)

#dim_course.to_csv(r"{filepath}", index=False)
#calendar.to_csv(r"{filepath}", index=False)
#employee.to_csv(r"{filepath}", index=False)
#manager.to_csv(r"{filepath}", index=False)
#department.to_csv(r"{filepath}", index=False)


