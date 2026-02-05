-- Hospital Resource Utilization Database Schema
-- For MySQL 8.0+

CREATE DATABASE IF NOT EXISTS hospital_analytics;
USE hospital_analytics;

-- Branch/Hospital Location
CREATE TABLE branches (
    branch_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    total_beds INT NOT NULL,
    icu_beds INT NOT NULL,
    general_beds INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Departments
CREATE TABLE departments (
    dept_id INT PRIMARY KEY AUTO_INCREMENT,
    dept_name VARCHAR(100) NOT NULL,
    dept_type ENUM('Cardiology', 'Oncology', 'Orthopedics', 'Pediatrics', 'Emergency', 'General Medicine') NOT NULL,
    branch_id INT,
    total_beds INT,
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
);

-- Doctors/Staff
CREATE TABLE doctors (
    doctor_id INT PRIMARY KEY AUTO_INCREMENT,
    doctor_name VARCHAR(100) NOT NULL,
    specialization VARCHAR(100),
    dept_id INT,
    branch_id INT,
    working_hours_per_week INT DEFAULT 40,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
);

-- Patients
CREATE TABLE patients (
    patient_id INT PRIMARY KEY AUTO_INCREMENT,
    patient_name VARCHAR(100) NOT NULL,
    age INT,
    gender ENUM('Male', 'Female', 'Other'),
    insurance_type ENUM('Government', 'Private', 'Self-Pay', 'Corporate'),
    contact_number VARCHAR(15),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Admissions
CREATE TABLE admissions (
    admission_id INT PRIMARY KEY AUTO_INCREMENT,
    patient_id INT NOT NULL,
    branch_id INT NOT NULL,
    dept_id INT NOT NULL,
    doctor_id INT NOT NULL,
    admission_date DATETIME NOT NULL,
    discharge_date DATETIME,
    admission_type ENUM('Emergency', 'Scheduled') NOT NULL,
    diagnosis_category VARCHAR(200),
    bed_type ENUM('ICU', 'General', 'Private', 'Semi-Private'),
    bed_number VARCHAR(20),
    status ENUM('Active', 'Discharged', 'Transferred') DEFAULT 'Active',
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
    INDEX idx_admission_date (admission_date),
    INDEX idx_discharge_date (discharge_date),
    INDEX idx_status (status)
);

-- Procedures
CREATE TABLE procedures (
    procedure_id INT PRIMARY KEY AUTO_INCREMENT,
    procedure_name VARCHAR(200) NOT NULL,
    procedure_type ENUM('Surgery', 'Diagnostic', 'Therapeutic', 'Emergency') NOT NULL,
    dept_id INT,
    base_cost DECIMAL(10, 2),
    avg_duration_minutes INT,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- Patient Procedures (linking table)
CREATE TABLE patient_procedures (
    record_id INT PRIMARY KEY AUTO_INCREMENT,
    admission_id INT NOT NULL,
    procedure_id INT NOT NULL,
    procedure_date DATETIME NOT NULL,
    doctor_id INT NOT NULL,
    duration_minutes INT,
    cost DECIMAL(10, 2),
    status ENUM('Scheduled', 'Completed', 'Cancelled') DEFAULT 'Scheduled',
    FOREIGN KEY (admission_id) REFERENCES admissions(admission_id),
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
    INDEX idx_procedure_date (procedure_date)
);

-- Billing
CREATE TABLE billing (
    bill_id INT PRIMARY KEY AUTO_INCREMENT,
    admission_id INT NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    room_charges DECIMAL(10, 2),
    procedure_charges DECIMAL(10, 2),
    medicine_charges DECIMAL(10, 2),
    lab_charges DECIMAL(10, 2),
    other_charges DECIMAL(10, 2),
    discount DECIMAL(10, 2) DEFAULT 0,
    insurance_coverage DECIMAL(10, 2) DEFAULT 0,
    amount_paid DECIMAL(12, 2) DEFAULT 0,
    payment_status ENUM('Pending', 'Partial', 'Paid') DEFAULT 'Pending',
    bill_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (admission_id) REFERENCES admissions(admission_id)
);

-- Patient Outcomes
CREATE TABLE outcomes (
    outcome_id INT PRIMARY KEY AUTO_INCREMENT,
    admission_id INT NOT NULL,
    outcome_type ENUM('Recovered', 'Improved', 'Transferred', 'Deceased', 'LAMA') NOT NULL,
    outcome_date DATETIME NOT NULL,
    readmission_flag BOOLEAN DEFAULT FALSE,
    readmission_within_30days BOOLEAN DEFAULT FALSE,
    notes TEXT,
    FOREIGN KEY (admission_id) REFERENCES admissions(admission_id),
    INDEX idx_outcome_date (outcome_date)
);

-- Doctor Schedule (for utilization tracking)
CREATE TABLE doctor_schedules (
    schedule_id INT PRIMARY KEY AUTO_INCREMENT,
    doctor_id INT NOT NULL,
    schedule_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    total_hours DECIMAL(4, 2),
    booked_hours DECIMAL(4, 2) DEFAULT 0,
    available_hours DECIMAL(4, 2),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
    INDEX idx_schedule_date (schedule_date)
);

-- Bed Occupancy Tracking (Daily Snapshot)
CREATE TABLE bed_occupancy_daily (
    record_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_id INT NOT NULL,
    dept_id INT,
    snapshot_date DATE NOT NULL,
    snapshot_hour INT,
    total_beds INT NOT NULL,
    occupied_beds INT NOT NULL,
    occupancy_rate DECIMAL(5, 2),
    icu_occupied INT DEFAULT 0,
    general_occupied INT DEFAULT 0,
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
    INDEX idx_snapshot_date (snapshot_date)
);

-- Resource Alerts
CREATE TABLE resource_alerts (
    alert_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_id INT NOT NULL,
    dept_id INT,
    alert_type ENUM('Bed_Shortage', 'Staff_Shortage', 'Equipment_Shortage', 'High_Occupancy') NOT NULL,
    severity ENUM('Low', 'Medium', 'High', 'Critical') NOT NULL,
    alert_message TEXT,
    alert_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
    INDEX idx_alert_date (alert_date)
);

-- Monthly Performance Summary
CREATE TABLE monthly_summary (
    summary_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_id INT NOT NULL,
    dept_id INT,
    summary_month DATE NOT NULL,
    total_admissions INT,
    total_discharges INT,
    avg_length_of_stay DECIMAL(5, 2),
    bed_occupancy_rate DECIMAL(5, 2),
    total_procedures INT,
    total_revenue DECIMAL(15, 2),
    cost_per_discharge DECIMAL(10, 2),
    readmission_rate DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
    INDEX idx_summary_month (summary_month)
);

-- Create Views for Common Analytics Queries

-- View: Current Active Admissions
CREATE VIEW v_active_admissions AS
SELECT 
    a.admission_id,
    a.patient_id,
    p.patient_name,
    p.age,
    p.gender,
    p.insurance_type,
    a.branch_id,
    b.branch_name,
    a.dept_id,
    d.dept_name,
    a.doctor_id,
    doc.doctor_name,
    a.admission_date,
    a.admission_type,
    a.diagnosis_category,
    a.bed_type,
    DATEDIFF(CURRENT_DATE, DATE(a.admission_date)) as length_of_stay_days
FROM admissions a
JOIN patients p ON a.patient_id = p.patient_id
JOIN branches b ON a.branch_id = b.branch_id
JOIN departments d ON a.dept_id = d.dept_id
JOIN doctors doc ON a.doctor_id = doc.doctor_id
WHERE a.status = 'Active';

-- View: Discharge Statistics
CREATE VIEW v_discharge_stats AS
SELECT 
    a.admission_id,
    a.patient_id,
    a.branch_id,
    b.branch_name,
    a.dept_id,
    d.dept_name,
    a.admission_date,
    a.discharge_date,
    DATEDIFF(a.discharge_date, a.admission_date) as length_of_stay,
    o.outcome_type,
    o.readmission_within_30days,
    bil.total_amount,
    bil.amount_paid
FROM admissions a
JOIN branches b ON a.branch_id = b.branch_id
JOIN departments d ON a.dept_id = d.dept_id
LEFT JOIN outcomes o ON a.admission_id = o.admission_id
LEFT JOIN billing bil ON a.admission_id = bil.admission_id
WHERE a.status = 'Discharged';

-- View: Department Performance
CREATE VIEW v_dept_performance AS
SELECT 
    d.dept_id,
    d.dept_name,
    d.branch_id,
    b.branch_name,
    COUNT(DISTINCT a.admission_id) as total_admissions,
    AVG(DATEDIFF(a.discharge_date, a.admission_date)) as avg_los,
    COUNT(DISTINCT CASE WHEN a.admission_type = 'Emergency' THEN a.admission_id END) as emergency_cases,
    COUNT(DISTINCT pp.procedure_id) as total_procedures
FROM departments d
JOIN branches b ON d.branch_id = b.branch_id
LEFT JOIN admissions a ON d.dept_id = a.dept_id
LEFT JOIN patient_procedures pp ON a.admission_id = pp.admission_id
GROUP BY d.dept_id, d.dept_name, d.branch_id, b.branch_name;
