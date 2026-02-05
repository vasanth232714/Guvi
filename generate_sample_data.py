"""
Sample Data Generator for Hospital Analytics Dashboard
Generates realistic hospital operational data for testing and demonstration
"""

import random
import datetime
from datetime import timedelta
import mysql.connector
from mysql.connector import Error

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'hospital_analytics',
    'user': 'hospital_admin',  # Update with your MySQL username
    'password': 'SecurePassword123!'   # Update with your MySQL password
}

# Sample data pools
BRANCH_NAMES = [
    'Mumbai Central Hospital', 'Delhi Metro Medical Center',
    'Bangalore Healthcare Hub', 'Chennai Regional Hospital'
]

LOCATIONS = [
    'Andheri West, Mumbai', 'Connaught Place, New Delhi',
    'Whitefield, Bangalore', 'T. Nagar, Chennai'
]

DEPT_TYPES = ['Cardiology', 'Oncology', 'Orthopedics', 'Pediatrics', 'Emergency', 'General Medicine']

DOCTOR_NAMES = [
    'Dr. Rajesh Kumar', 'Dr. Priya Sharma', 'Dr. Amit Patel', 'Dr. Sneha Reddy',
    'Dr. Vikram Singh', 'Dr. Anjali Gupta', 'Dr. Sanjay Desai', 'Dr. Kavita Rao',
    'Dr. Rahul Verma', 'Dr. Meera Iyer', 'Dr. Arun Nair', 'Dr. Pooja Menon',
    'Dr. Suresh Bhat', 'Dr. Divya Krishnan', 'Dr. Harish Joshi', 'Dr. Nisha Shah'
]

PATIENT_FIRST_NAMES = [
    'Arjun', 'Priya', 'Rahul', 'Anjali', 'Vikram', 'Sneha', 'Aditya', 'Kavya',
    'Rohan', 'Ishita', 'Karthik', 'Meera', 'Siddharth', 'Pooja', 'Nikhil', 'Divya'
]

PATIENT_LAST_NAMES = [
    'Sharma', 'Patel', 'Kumar', 'Reddy', 'Singh', 'Gupta', 'Rao', 'Iyer',
    'Nair', 'Desai', 'Verma', 'Menon', 'Bhat', 'Joshi', 'Shah', 'Krishnan'
]

DIAGNOSES = {
    'Cardiology': ['Acute Myocardial Infarction', 'Heart Failure', 'Arrhythmia', 'Hypertension Crisis', 'Coronary Artery Disease'],
    'Oncology': ['Breast Cancer', 'Lung Cancer', 'Colorectal Cancer', 'Leukemia', 'Lymphoma'],
    'Orthopedics': ['Hip Fracture', 'Knee Replacement', 'Spinal Injury', 'Sports Injury', 'Arthritis'],
    'Pediatrics': ['Pneumonia', 'Viral Fever', 'Gastroenteritis', 'Asthma', 'Dehydration'],
    'Emergency': ['Trauma', 'Poisoning', 'Acute Abdomen', 'Head Injury', 'Burns'],
    'General Medicine': ['Diabetes', 'Respiratory Infection', 'Gastritis', 'Fever', 'Hypertension']
}

PROCEDURES = {
    'Cardiology': [('Angioplasty', 'Surgery', 180000, 120), ('ECG', 'Diagnostic', 1500, 30), 
                   ('Echocardiogram', 'Diagnostic', 3500, 45), ('Cardiac Catheterization', 'Diagnostic', 50000, 90)],
    'Oncology': [('Chemotherapy Session', 'Therapeutic', 45000, 240), ('Radiation Therapy', 'Therapeutic', 35000, 60),
                 ('Biopsy', 'Diagnostic', 15000, 45), ('PET Scan', 'Diagnostic', 25000, 90)],
    'Orthopedics': [('Hip Replacement', 'Surgery', 250000, 180), ('Knee Arthroscopy', 'Surgery', 120000, 90),
                    ('Fracture Fixation', 'Surgery', 80000, 120), ('X-Ray', 'Diagnostic', 800, 15)],
    'Pediatrics': [('Vaccination', 'Therapeutic', 500, 15), ('Blood Test', 'Diagnostic', 1200, 30),
                   ('Nebulization', 'Therapeutic', 800, 30), ('IV Fluid Administration', 'Therapeutic', 2000, 60)],
    'Emergency': [('Trauma Surgery', 'Emergency', 150000, 240), ('Emergency Stabilization', 'Emergency', 25000, 60),
                  ('CT Scan', 'Diagnostic', 8000, 45), ('Emergency Suturing', 'Emergency', 5000, 30)],
    'General Medicine': [('Blood Test Panel', 'Diagnostic', 2500, 30), ('Ultrasound', 'Diagnostic', 2000, 30),
                         ('IV Antibiotic', 'Therapeutic', 3000, 60), ('Consultation', 'Diagnostic', 800, 30)]
}

def create_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def generate_dates(start_date, num_days=180):
    """Generate date range for data"""
    return [start_date + timedelta(days=x) for x in range(num_days)]

def insert_branches(cursor):
    """Insert hospital branches"""
    print("Inserting branches...")
    for i, (name, location) in enumerate(zip(BRANCH_NAMES, LOCATIONS)):
        total_beds = random.randint(200, 400)
        icu_beds = int(total_beds * 0.15)
        general_beds = total_beds - icu_beds
        
        cursor.execute("""
            INSERT INTO branches (branch_name, location, total_beds, icu_beds, general_beds)
            VALUES (%s, %s, %s, %s, %s)
        """, (name, location, total_beds, icu_beds, general_beds))
    print(f"Inserted {len(BRANCH_NAMES)} branches")

def insert_departments(cursor):
    """Insert departments for each branch"""
    print("Inserting departments...")
    cursor.execute("SELECT branch_id FROM branches")
    branches = cursor.fetchall()
    
    count = 0
    for branch_id in branches:
        for dept_type in DEPT_TYPES:
            beds = random.randint(30, 80)
            cursor.execute("""
                INSERT INTO departments (dept_name, dept_type, branch_id, total_beds)
                VALUES (%s, %s, %s, %s)
            """, (dept_type, dept_type, branch_id[0], beds))
            count += 1
    print(f"Inserted {count} departments")

def insert_doctors(cursor):
    print("Inserting doctors...")
    cursor.execute("SELECT dept_id, dept_type, branch_id FROM departments")
    departments = cursor.fetchall()

    count = 0

    for dept_id, dept_type, branch_id in departments:
        num_doctors = random.randint(3, 5)

        for _ in range(num_doctors):
            name = random.choice(DOCTOR_NAMES)
            working_hours = random.choice([40, 48, 50, 60])

            cursor.execute("""
                INSERT INTO doctors
                (doctor_name, specialization, dept_id, branch_id, working_hours_per_week)
                VALUES (%s, %s, %s, %s, %s)
            """, (name, dept_type, dept_id, branch_id, working_hours))

            count += 1

    print(f"Inserted {count} doctors")


def insert_patients(cursor, num_patients=2000):
    """Insert patient records"""
    print(f"Inserting {num_patients} patients...")
    insurance_types = ['Government', 'Private', 'Self-Pay', 'Corporate']
    genders = ['Male', 'Female', 'Other']
    
    for i in range(num_patients):
        first_name = random.choice(PATIENT_FIRST_NAMES)
        last_name = random.choice(PATIENT_LAST_NAMES)
        name = f"{first_name} {last_name}"
        age = random.randint(1, 90)
        gender = random.choice(genders)
        insurance = random.choice(insurance_types)
        contact = f"+91{random.randint(7000000000, 9999999999)}"
        
        cursor.execute("""
            INSERT INTO patients (patient_name, age, gender, insurance_type, contact_number)
            VALUES (%s, %s, %s, %s, %s)
        """, (name, age, gender, insurance, contact))
    print(f"Inserted {num_patients} patients")

def insert_procedures_master(cursor):
    """Insert procedure master data"""
    print("Inserting procedure master data...")
    cursor.execute("SELECT dept_id, dept_type FROM departments")
    departments = {dept_type: dept_id for dept_id, dept_type in cursor.fetchall()}
    
    count = 0
    for dept_type, procedures in PROCEDURES.items():
        dept_id = departments.get(dept_type)
        if dept_id:
            for proc_name, proc_type, cost, duration in procedures:
                cursor.execute("""
                    INSERT INTO procedures (procedure_name, procedure_type, dept_id, base_cost, avg_duration_minutes)
                    VALUES (%s, %s, %s, %s, %s)
                """, (proc_name, proc_type, dept_id, cost, duration))
                count += 1
    print(f"Inserted {count} procedures")
from decimal import Decimal

def insert_admissions_and_related(cursor, start_date, num_days=180):
    """Insert admissions, procedures, billing, and outcomes"""
    print(f"Generating admissions data for {num_days} days...")
    
    cursor.execute("SELECT patient_id FROM patients")
    patient_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT dept_id, dept_type, branch_id FROM departments")
    departments = cursor.fetchall()
    
    cursor.execute("SELECT doctor_id, dept_id FROM doctors")
    doctors_by_dept = {}
    for doctor_id, dept_id in cursor.fetchall():
        if dept_id not in doctors_by_dept:
            doctors_by_dept[dept_id] = []
        doctors_by_dept[dept_id].append(doctor_id)
    
    cursor.execute("SELECT procedure_id, dept_id, base_cost, avg_duration_minutes FROM procedures")
    procedures_by_dept = {}
    for proc_id, dept_id, cost, duration in cursor.fetchall():
        if dept_id not in procedures_by_dept:
            procedures_by_dept[dept_id] = []
        procedures_by_dept[dept_id].append((proc_id, cost, duration))
    
    admission_count = 0
    outcome_types = ['Recovered', 'Improved', 'Transferred', 'Deceased']
    outcome_weights = [0.65, 0.25, 0.07, 0.03]
    
    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        
        # Vary admissions by day (weekend effect)
        is_weekend = current_date.weekday() >= 5
        base_admissions = random.randint(15, 30) if not is_weekend else random.randint(8, 18)
        
        for _ in range(base_admissions):
            dept_id, dept_type, branch_id = random.choice(departments)
            patient_id = random.choice(patient_ids)
            doctor_id = random.choice(doctors_by_dept.get(dept_id, [1]))
            
            admission_type = random.choices(['Emergency', 'Scheduled'], weights=[0.35, 0.65])[0]
            diagnosis = random.choice(DIAGNOSES.get(dept_type, ['General Condition']))
            bed_type = random.choices(['ICU', 'General', 'Private', 'Semi-Private'], 
                                     weights=[0.15, 0.50, 0.20, 0.15])[0]
            
            admission_time = current_date + timedelta(hours=random.randint(0, 23), 
                                                     minutes=random.randint(0, 59))
            
            # Determine if discharged
            los_days = random.choices([1, 2, 3, 4, 5, 6, 7, 10, 14], 
                                     weights=[0.05, 0.15, 0.20, 0.18, 0.15, 0.10, 0.08, 0.06, 0.03])[0]
            discharge_date = admission_time + timedelta(days=los_days)
            
            status = 'Discharged' if discharge_date <= datetime.datetime.now() else 'Active'
            discharge_date_final = discharge_date if status == 'Discharged' else None
            
            cursor.execute("""
                INSERT INTO admissions 
                (patient_id, branch_id, dept_id, doctor_id, admission_date, discharge_date, 
                 admission_type, diagnosis_category, bed_type, bed_number, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (patient_id, branch_id, dept_id, doctor_id, admission_time, discharge_date_final,
                  admission_type, diagnosis, bed_type, f"B-{random.randint(101, 499)}", status))
            
            admission_id = cursor.lastrowid
            admission_count += 1
            
            # Add procedures
            procs = procedures_by_dept.get(dept_id, [])
            if procs:
                num_procedures = random.randint(1, 3)
                total_proc_cost = 0
                
                for proc_num in range(num_procedures):
                    proc_id, base_cost, duration = random.choice(procs)
                    proc_date = admission_time + timedelta(days=random.randint(0, min(los_days, 3)))
                    actual_cost = base_cost * Decimal(str(random.uniform(0.9, 1.2)))

                    total_proc_cost += actual_cost
                    
                    cursor.execute("""
                        INSERT INTO patient_procedures 
                        (admission_id, procedure_id, procedure_date, doctor_id, duration_minutes, cost, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (admission_id, proc_id, proc_date, doctor_id, duration, actual_cost, 'Completed'))
            
            # Add billing
            room_charges = los_days * random.randint(2000, 8000)
            procedure_charges = total_proc_cost if 'total_proc_cost' in locals() else 0
            medicine_charges = los_days * random.randint(1000, 3000)
            lab_charges = random.randint(2000, 10000)
            other_charges = random.randint(1000, 5000)
            
            total_amount = room_charges + procedure_charges + medicine_charges + lab_charges + other_charges
            discount = total_amount *Decimal(str(random.uniform(0.9, 1.2)))
            
            # Get insurance type
            cursor.execute("SELECT insurance_type FROM patients WHERE patient_id = %s", (patient_id,))
            insurance_type = cursor.fetchone()[0]
            insurance_coverage = total_amount * Decimal(str(random.uniform(0.5, 0.8))) if insurance_type != 'Self-Pay' else 0
            
            amount_paid = total_amount - discount if status == 'Discharged' else total_amount * Decimal(str(random.uniform(0.3, 0.7)))
            payment_status = 'Paid' if amount_paid >= (total_amount - discount - insurance_coverage) else 'Partial'
            
            cursor.execute("""
                INSERT INTO billing 
                (admission_id, total_amount, room_charges, procedure_charges, medicine_charges, 
                 lab_charges, other_charges, discount, insurance_coverage, amount_paid, payment_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (admission_id, total_amount, room_charges, procedure_charges, medicine_charges,
                  lab_charges, other_charges, discount, insurance_coverage, amount_paid, payment_status))
            
            # Add outcome for discharged patients
            if status == 'Discharged':
                outcome_type = random.choices(outcome_types, weights=outcome_weights)[0]
                readmission_30 = random.random() < 0.12  # 12% readmission rate
                
                cursor.execute("""
                    INSERT INTO outcomes 
                    (admission_id, outcome_type, outcome_date, readmission_flag,readmission_within_30days)
                    VALUES (%s, %s, %s, %s, %s)
                """, (admission_id, outcome_type, discharge_date_final, readmission_30, readmission_30))
    
    print(f"Inserted {admission_count} admissions with related data")

def insert_bed_occupancy_data(cursor, start_date, num_days=180):
    """Insert daily bed occupancy snapshots"""
    print(f"Generating bed occupancy data for {num_days} days...")
    
    cursor.execute("SELECT branch_id, total_beds FROM branches")
    branches = cursor.fetchall()
    
    cursor.execute("SELECT dept_id, branch_id, total_beds FROM departments")
    departments = cursor.fetchall()
    
    count = 0
    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        
        for branch_id, total_beds in branches:
            occupied = int(total_beds * random.uniform(0.60, 0.95))
            occupancy_rate = (occupied / total_beds) * 100
            icu_occupied = int(occupied * random.uniform(0.10, 0.20))
            general_occupied = occupied - icu_occupied
            
            cursor.execute("""
                INSERT INTO bed_occupancy_daily 
                (branch_id, snapshot_date, snapshot_hour, total_beds, occupied_beds, 
                 occupancy_rate, icu_occupied, general_occupied)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (branch_id, current_date, 12, total_beds, occupied, occupancy_rate, 
                  icu_occupied, general_occupied))
            count += 1
        
        for dept_id, branch_id, dept_beds in departments:
            occupied = int(dept_beds * random.uniform(0.55, 0.92))
            occupancy_rate = (occupied / dept_beds) * 100
            
            cursor.execute("""
                INSERT INTO bed_occupancy_daily 
                (branch_id, dept_id, snapshot_date, snapshot_hour, total_beds, occupied_beds, occupancy_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (branch_id, dept_id, current_date, 12, dept_beds, occupied, occupancy_rate))
            count += 1
    
    print(f"Inserted {count} bed occupancy records")

def generate_resource_alerts(cursor):
    """Generate sample resource alerts"""
    print("Generating resource alerts...")
    
    cursor.execute("SELECT branch_id FROM branches")
    branches = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT dept_id FROM departments")
    departments = [row[0] for row in cursor.fetchall()]
    
    alert_types = ['Bed_Shortage', 'Staff_Shortage', 'Equipment_Shortage', 'High_Occupancy']
    severities = ['Low', 'Medium', 'High', 'Critical']
    messages = {
        'Bed_Shortage': 'ICU beds running low - only 2 available',
        'Staff_Shortage': 'Insufficient nursing staff for night shift',
        'Equipment_Shortage': 'Ventilator availability critical',
        'High_Occupancy': 'Department occupancy exceeds 90%'
    }
    
    num_alerts = random.randint(10, 20)
    for _ in range(num_alerts):
        branch_id = random.choice(branches)
        dept_id = random.choice(departments) if random.random() > 0.3 else None
        alert_type = random.choice(alert_types)
        severity = random.choice(severities)
        alert_date = datetime.datetime.now() - timedelta(days=random.randint(0, 30))
        resolved = random.random() > 0.4
        
        cursor.execute("""
            INSERT INTO resource_alerts 
            (branch_id, dept_id, alert_type, severity, alert_message, alert_date, resolved)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (branch_id, dept_id, alert_type, severity, messages[alert_type], alert_date, resolved))
    
    print(f"Inserted {num_alerts} resource alerts")

def main():
    """Main execution function"""
    connection = create_connection()
    if not connection:
        return
    
    cursor = connection.cursor()
    
    try:
        # Start date for data generation (6 months ago)
        start_date = datetime.datetime.now() - timedelta(days=180)
        
        print("\n=== Starting Data Generation ===\n")
        
        # Generate data
        insert_branches(cursor)
        connection.commit()
        
        insert_departments(cursor)
        connection.commit()
        
        insert_doctors(cursor)
        connection.commit()
        
        insert_patients(cursor, num_patients=2000)
        connection.commit()
        
        insert_procedures_master(cursor)
        connection.commit()
        
        insert_admissions_and_related(cursor, start_date, num_days=180)
        connection.commit()
        
        insert_bed_occupancy_data(cursor, start_date, num_days=180)
        connection.commit()
        
        generate_resource_alerts(cursor)
        connection.commit()
        
        print("\n=== Data Generation Complete! ===")
        print("\nDatabase Statistics:")
        
        # Print summary statistics
        cursor.execute("SELECT COUNT(*) FROM branches")
        print(f"Branches: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM departments")
        print(f"Departments: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM doctors")
        print(f"Doctors: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM patients")
        print(f"Patients: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM admissions")
        print(f"Admissions: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM patient_procedures")
        print(f"Procedures: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM bed_occupancy_daily")
        print(f"Bed Occupancy Records: {cursor.fetchone()[0]}")
        
    except Error as e:
        print(f"Error during data generation: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()