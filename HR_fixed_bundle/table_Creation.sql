/* create database: */
--step 1
CREATE DATABASE Capstone_HR;

--step 2
/*dim tables */
    CREATE TABLE DimEmployee(
        EmployeeKey INT PRIMARY KEY,
        Employee_ID INT,
        Name NVARCHAR(100),
        Birth_Date DATE,
        Age INT,
        Job_Title NVARCHAR(100),
        Department NVARCHAR(100),
        Office_Location NVARCHAR(100),
        Hire_Date DATE,
        Salary_Band INT,
        Gender NVARCHAR(20),
        Education_Level NVARCHAR(50),
        Remote_Eligibility INT,
        Preferred_Work_Mode NVARCHAR(50),
        Department_Key INT,
        Tenure_Years Decimal (5,2),
        Ethnicity NVARCHAR(50),
        Separation_Date DATE,
        Separation_Reason NVARCHAR(50)
    );

    CREATE TABLE DimDepartment (
        DepartmentKey INT PRIMARY KEY,
        Department NVARCHAR(25),
        Department_Key INT
    );

    CREATE TABLE DimManager(
        ManagerKey INT PRIMARY KEY,
        Manager_ID INT,
        Manager_Name NVARCHAR(100),
        Manager_Department NVARCHAR(50)
    );

    CREATE TABLE DimCalendar(
        DateKey INT PRIMARY KEY,
        Date DATE,
        Year INT,
        Quarter INT,
        Month INT,
        Day INT,
        Weekday INT,
        Is_Weekend INT,
        Week_Of_Year INT,
        Month_Name NVARCHAR(15),
        DOW_Name NVARCHAR(15)
        );

    CREATE TABLE DimCourse(
        CourseKey INT PRIMARY KEY,
        Course_Code NVARCHAR(20),
        Course_Name NVARCHAR(100)
    ); 

--step 3
/*fact tables */
CREATE TABLE FAttendance(
    FactAttendanceID INT PRIMARY KEY,
    EmployeeKey INT, 
    DateKey INT,
    HoursWorked DECIMAL(5,2),
    Remote_Flag NVARCHAR(20),
    FOREIGN KEY (EmployeeKey) REFERENCES dbo.DimEmployee(EmployeeKey),
    FOREIGN KEY (DateKey) REFERENCES dbo.DimCalendar(DateKey)
);

/* REMOVING THIS TABLE FOR NOW.
CREATE TABLE FPTO(
    FactPTOID INT PRIMARY KEY,
    EmployeeKey INT,
    DateKey INT,
    Hours INT,
    PTO_Type NVARCHAR(30),
    FOREIGN KEY (EmployeeKey) REFERENCES dbo.DimEmployee(EmployeeKey),
    FOREIGN KEY (DateKey) REFERENCES dbo.DimCalendar(DateKey)
);*/

CREATE TABLE FRequest(
    FactRequestID INT PRIMARY KEY,
    EmployeeKey INT, 
    RequestDateKey INT,
    StartDateKey INT,
    EndDateKey INT,
    PTO_Type NVARCHAR(30),
    Approved NVARCHAR(15),
    Reason NVARCHAR(50),
    Hours_Taken INT,
    FOREIGN KEY (EmployeeKey) REFERENCES dbo.DimEmployee(EmployeeKey),
    FOREIGN KEY (RequestDateKey) REFERENCES dbo.DimCalendar(DateKey),
    FOREIGN KEY (StartDateKey) REFERENCES dbo.DimCalendar(DateKey),
    FOREIGN KEY (EndDateKey) REFERENCES dbo.DimCalendar(DateKey)
);


CREATE TABLE FSurvey(
    FactSurveyID INT PRIMARY KEY,
    EmployeeKey INT,
    DateKey INT,
    Satisfaction_Score INT,
    Engagement_Index INT, 
    Manager_Effectiveness INT,
    ENPS INT,
    Comments NVARCHAR(200),
    FOREIGN KEY (EmployeeKey) REFERENCES dbo.DimEmployee(EmployeeKey),
    FOREIGN KEY (DateKey) REFERENCES dbo.DimCalendar(DateKey)
);


CREATE TABLE FTrain(
    FactTrainingID INT PRIMARY KEY,
    EmployeeKey INT, 
    CourseKey INT, 
    Score INT,
    FOREIGN KEY (EmployeeKey) REFERENCES dbo.DimEmployee(EmployeeKey),
    FOREIGN KEY (CourseKey) REFERENCES dbo.DimCourse(CourseKey)
);