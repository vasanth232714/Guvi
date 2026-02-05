"""
Hospital Analytics Dashboard - Flask Backend API
Provides RESTful endpoints for hospital resource utilization analytics
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import json
from decimal import Decimal

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'hospital_analytics',
    'user': 'hospital_admin',
    'password': 'SecurePassword123!'  # Update with your MySQL password
}

def get_db_connection():
    """Create and return database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Database connection error: {e}")
        return None

def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# ============== CORE KPI ENDPOINTS ==============

@app.route('/api/kpis/summary', methods=['GET'])
def get_kpi_summary():
    """Get overall KPI summary with filters"""
    branch_id = request.args.get('branch_id')
    dept_id = request.args.get('dept_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    # Build WHERE clause
    where_conditions = []
    params = []
    
    if branch_id:
        where_conditions.append("a.branch_id = %s")
        params.append(branch_id)
    if dept_id:
        where_conditions.append("a.dept_id = %s")
        params.append(dept_id)
    if start_date:
        where_conditions.append("a.admission_date >= %s")
        params.append(start_date)
    if end_date:
        where_conditions.append("a.admission_date <= %s")
        params.append(end_date)
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Average Length of Stay (ALOS)
    query = f"""
        SELECT AVG(DATEDIFF(COALESCE(discharge_date, CURRENT_DATE), admission_date)) as alos
        FROM admissions a
        {where_clause}
    """
    cursor.execute(query, params)
    alos_result = cursor.fetchone()
    alos = float(alos_result['alos']) if alos_result['alos'] else 0
    
    # Bed Occupancy Rate (current)
    if branch_id:
        cursor.execute("""
            SELECT AVG(occupancy_rate) as avg_occupancy
            FROM bed_occupancy_daily
            WHERE branch_id = %s AND snapshot_date = CURRENT_DATE
        """, (branch_id,))
    else:
        cursor.execute("""
            SELECT AVG(occupancy_rate) as avg_occupancy
            FROM bed_occupancy_daily
            WHERE snapshot_date = CURRENT_DATE
        """)
    occupancy_result = cursor.fetchone()
    bed_occupancy = float(occupancy_result['avg_occupancy']) if occupancy_result['avg_occupancy'] else 0
    
    # Patient Counts
    query = f"""
        SELECT 
            COUNT(*) as total_admissions,
            SUM(CASE WHEN status = 'Discharged' THEN 1 ELSE 0 END) as total_discharges,
            SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active_patients
        FROM admissions a
        {where_clause}
    """
    cursor.execute(query, params)
    counts = cursor.fetchone()
    
    # Readmission Rate (30-day)
    query = f"""
        SELECT 
            COUNT(*) as total_discharges,
            SUM(CASE WHEN o.readmission_within_30days = TRUE THEN 1 ELSE 0 END) as readmissions
        FROM admissions a
        LEFT JOIN outcomes o ON a.admission_id = o.admission_id
        {where_clause} AND a.status = 'Discharged'
    """
    cursor.execute(query, params)
    readmission_data = cursor.fetchone()
    total_discharges = readmission_data['total_discharges']
    readmission_rate = (readmission_data['readmissions'] / total_discharges * 100) if total_discharges > 0 else 0
    
    # Procedure Volume
    query = f"""
        SELECT COUNT(*) as procedure_count
        FROM patient_procedures pp
        JOIN admissions a ON pp.admission_id = a.admission_id
        {where_clause}
    """
    cursor.execute(query, params)
    procedure_count = cursor.fetchone()['procedure_count']
    
    # Emergency vs Scheduled
    query = f"""
        SELECT 
            admission_type,
            COUNT(*) as count
        FROM admissions a
        {where_clause}
        GROUP BY admission_type
    """
    cursor.execute(query, params)
    admission_types = {row['admission_type']: row['count'] for row in cursor.fetchall()}
    
    # Cost per Patient
    query = f"""
        SELECT AVG(b.total_amount) as avg_cost
        FROM billing b
        JOIN admissions a ON b.admission_id = a.admission_id
        {where_clause}
    """
    cursor.execute(query, params)
    cost_result = cursor.fetchone()
    avg_cost = float(cost_result['avg_cost']) if cost_result['avg_cost'] else 0
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'alos': round(alos, 2),
        'bed_occupancy_rate': round(bed_occupancy, 2),
        'total_admissions': counts['total_admissions'],
        'total_discharges': counts['total_discharges'],
        'active_patients': counts['active_patients'],
        'readmission_rate': round(readmission_rate, 2),
        'procedure_volume': procedure_count,
        'emergency_cases': admission_types.get('Emergency', 0),
        'scheduled_cases': admission_types.get('Scheduled', 0),
        'avg_cost_per_patient': round(avg_cost, 2)
    })

@app.route('/api/trends/admissions', methods=['GET'])
def get_admission_trends():
    """Get admission trends over time"""
    period = request.args.get('period', 'daily')  # daily, weekly, monthly
    branch_id = request.args.get('branch_id')
    dept_id = request.args.get('dept_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    if period == 'daily':
        date_format = '%Y-%m-%d'
        group_by = "DATE(admission_date)"
    elif period == 'weekly':
        date_format = '%Y-%u'
        group_by = "YEAR(admission_date), WEEK(admission_date)"
    else:  # monthly
        date_format = '%Y-%m'
        group_by = "YEAR(admission_date), MONTH(admission_date)"
    
    where_conditions = ["admission_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)"]
    params = []
    
    if branch_id:
        where_conditions.append("branch_id = %s")
        params.append(branch_id)
    if dept_id:
        where_conditions.append("dept_id = %s")
        params.append(dept_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            DATE_FORMAT(admission_date, '{date_format}') as period,
            COUNT(*) as total_admissions,
            SUM(CASE WHEN admission_type = 'Emergency' THEN 1 ELSE 0 END) as emergency_admissions,
            SUM(CASE WHEN admission_type = 'Scheduled' THEN 1 ELSE 0 END) as scheduled_admissions
        FROM admissions
        {where_clause}
        GROUP BY {group_by}
        ORDER BY admission_date
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/trends/bed-occupancy', methods=['GET'])
def get_bed_occupancy_trends():
    """Get bed occupancy trends"""
    branch_id = request.args.get('branch_id')
    dept_id = request.args.get('dept_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_conditions = ["snapshot_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)"]
    params = []
    
    if branch_id:
        where_conditions.append("branch_id = %s")
        params.append(branch_id)
    if dept_id:
        where_conditions.append("dept_id = %s")
        params.append(dept_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            DATE_FORMAT(snapshot_date, '%Y-%m-%d') as date,
            AVG(occupancy_rate) as avg_occupancy,
            AVG(icu_occupied) as avg_icu_occupied,
            AVG(general_occupied) as avg_general_occupied
        FROM bed_occupancy_daily
        {where_clause}
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Convert Decimal to float
    for row in results:
        if row['avg_occupancy']:
            row['avg_occupancy'] = float(row['avg_occupancy'])
        if row['avg_icu_occupied']:
            row['avg_icu_occupied'] = float(row['avg_icu_occupied'])
        if row['avg_general_occupied']:
            row['avg_general_occupied'] = float(row['avg_general_occupied'])
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/departments/comparison', methods=['GET'])
def get_department_comparison():
    """Compare metrics across departments"""
    branch_id = request.args.get('branch_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_clause = f"WHERE d.branch_id = {branch_id}" if branch_id else ""
    
    query = f"""
        SELECT 
            d.dept_name,
            COUNT(DISTINCT a.admission_id) as total_admissions,
            AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
            COUNT(DISTINCT pp.procedure_id) as total_procedures,
            SUM(CASE WHEN a.admission_type = 'Emergency' THEN 1 ELSE 0 END) as emergency_cases,
            AVG(b.total_amount) as avg_cost
        FROM departments d
        LEFT JOIN admissions a ON d.dept_id = a.dept_id
        LEFT JOIN patient_procedures pp ON a.admission_id = pp.admission_id
        LEFT JOIN billing b ON a.admission_id = b.admission_id
        {where_clause}
        GROUP BY d.dept_id, d.dept_name
        ORDER BY total_admissions DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert Decimal to float
    for row in results:
        if row['avg_los']:
            row['avg_los'] = float(row['avg_los'])
        if row['avg_cost']:
            row['avg_cost'] = float(row['avg_cost'])
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/branches/comparison', methods=['GET'])
def get_branch_comparison():
    """Compare metrics across hospital branches"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    query = """
        SELECT 
            b.branch_name,
            b.total_beds,
            COUNT(DISTINCT a.admission_id) as total_admissions,
            AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
            AVG(bod.occupancy_rate) as avg_occupancy,
            SUM(bil.total_amount) as total_revenue,
            AVG(bil.total_amount) as avg_revenue_per_patient
        FROM branches b
        LEFT JOIN admissions a ON b.branch_id = a.branch_id
        LEFT JOIN bed_occupancy_daily bod ON b.branch_id = bod.branch_id 
            AND bod.snapshot_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        LEFT JOIN billing bil ON a.admission_id = bil.admission_id
        GROUP BY b.branch_id, b.branch_name, b.total_beds
        ORDER BY total_admissions DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert Decimal to float
    for row in results:
        if row['avg_los']:
            row['avg_los'] = float(row['avg_los'])
        if row['avg_occupancy']:
            row['avg_occupancy'] = float(row['avg_occupancy'])
        if row['total_revenue']:
            row['total_revenue'] = float(row['total_revenue'])
        if row['avg_revenue_per_patient']:
            row['avg_revenue_per_patient'] = float(row['avg_revenue_per_patient'])
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/doctor-utilization', methods=['GET'])
def get_doctor_utilization():
    """Get doctor utilization statistics"""
    dept_id = request.args.get('dept_id')
    branch_id = request.args.get('branch_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_conditions = []
    params = []
    
    if dept_id:
        where_conditions.append("doc.dept_id = %s")
        params.append(dept_id)
    if branch_id:
        where_conditions.append("doc.branch_id = %s")
        params.append(branch_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    query = f"""
        SELECT 
            doc.doctor_name,
            dep.dept_name,
            doc.working_hours_per_week,
            COUNT(DISTINCT a.admission_id) as patients_handled,
            COUNT(DISTINCT pp.procedure_id) as procedures_performed,
            AVG(pp.duration_minutes) as avg_procedure_duration
        FROM doctors doc
        LEFT JOIN departments dep ON doc.dept_id = dep.dept_id
        LEFT JOIN admissions a ON doc.doctor_id = a.doctor_id 
            AND a.admission_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        LEFT JOIN patient_procedures pp ON doc.doctor_id = pp.doctor_id
            AND pp.procedure_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
        {where_clause}
        GROUP BY doc.doctor_id, doc.doctor_name, dep.dept_name, doc.working_hours_per_week
        ORDER BY patients_handled DESC
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Calculate utilization percentage (simplified)
    for row in results:
        if row['avg_procedure_duration']:
            row['avg_procedure_duration'] = float(row['avg_procedure_duration'])
        # Estimate utilization based on patients and procedures
        weekly_hours = row['working_hours_per_week']
        estimated_hours = (row['patients_handled'] * 0.5 + row['procedures_performed'] * 1.5)
        row['utilization_percentage'] = min(100, round((estimated_hours / weekly_hours * 100), 2))
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/outcomes/summary', methods=['GET'])
def get_outcomes_summary():
    """Get patient outcome statistics"""
    branch_id = request.args.get('branch_id')
    dept_id = request.args.get('dept_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_conditions = []
    params = []
    
    if branch_id:
        where_conditions.append("a.branch_id = %s")
        params.append(branch_id)
    if dept_id:
        where_conditions.append("a.dept_id = %s")
        params.append(dept_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    if where_clause:
        where_clause += " AND o.outcome_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)"
    else:
        where_clause = "WHERE o.outcome_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)"
    
    query = f"""
        SELECT 
            o.outcome_type,
            COUNT(*) as count
        FROM outcomes o
        JOIN admissions a ON o.admission_id = a.admission_id
        {where_clause}
        GROUP BY o.outcome_type
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/alerts/active', methods=['GET'])
def get_active_alerts():
    """Get active resource alerts"""
    branch_id = request.args.get('branch_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_conditions = ["resolved = FALSE"]
    params = []
    
    if branch_id:
        where_conditions.append("ra.branch_id = %s")
        params.append(branch_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            ra.alert_id,
            ra.alert_type,
            ra.severity,
            ra.alert_message,
            ra.alert_date,
            b.branch_name,
            d.dept_name
        FROM resource_alerts ra
        JOIN branches b ON ra.branch_id = b.branch_id
        LEFT JOIN departments d ON ra.dept_id = d.dept_id
        {where_clause}
        ORDER BY 
            FIELD(ra.severity, 'Critical', 'High', 'Medium', 'Low'),
            ra.alert_date DESC
        LIMIT 50
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(results)

@app.route('/api/peak-hours', methods=['GET'])
def get_peak_hours():
    """Get peak admission hours/days for staffing optimization"""
    branch_id = request.args.get('branch_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_clause = f"WHERE branch_id = {branch_id}" if branch_id else ""
    if where_clause:
        where_clause += " AND admission_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)"
    else:
        where_clause = "WHERE admission_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)"
    
    # Peak hours
    query = f"""
        SELECT 
            HOUR(admission_date) as hour,
            COUNT(*) as admission_count
        FROM admissions
        {where_clause}
        GROUP BY HOUR(admission_date)
        ORDER BY admission_count DESC
        LIMIT 10
    """
    
    cursor.execute(query)
    peak_hours = cursor.fetchall()
    
    # Peak days of week
    query = f"""
        SELECT 
            DAYNAME(admission_date) as day_name,
            DAYOFWEEK(admission_date) as day_number,
            COUNT(*) as admission_count
        FROM admissions
        {where_clause}
        GROUP BY DAYOFWEEK(admission_date), DAYNAME(admission_date)
        ORDER BY admission_count DESC
    """
    
    cursor.execute(query)
    peak_days = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'peak_hours': peak_hours,
        'peak_days': peak_days
    })

@app.route('/api/filters/options', methods=['GET'])
def get_filter_options():
    """Get available filter options (branches, departments)"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT branch_id, branch_name, location FROM branches ORDER BY branch_name")
    branches = cursor.fetchall()
    
    cursor.execute("SELECT dept_id, dept_name, dept_type, branch_id FROM departments ORDER BY dept_name")
    departments = cursor.fetchall()
    
    cursor.execute("SELECT DISTINCT diagnosis_category FROM admissions WHERE diagnosis_category IS NOT NULL")
    diagnoses = [row['diagnosis_category'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT insurance_type FROM patients")
    insurance_types = [row['insurance_type'] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'branches': branches,
        'departments': departments,
        'diagnoses': diagnoses,
        'insurance_types': insurance_types
    })

@app.route('/api/export/monthly-report', methods=['GET'])
def export_monthly_report():
    """Generate monthly performance report data"""
    month = request.args.get('month')  # Format: YYYY-MM
    branch_id = request.args.get('branch_id')
    
    if not month:
        return jsonify({'error': 'Month parameter required (format: YYYY-MM)'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    where_conditions = [
        f"DATE_FORMAT(a.admission_date, '%Y-%m') = '{month}'"
    ]
    params = []
    
    if branch_id:
        where_conditions.append("a.branch_id = %s")
        params.append(branch_id)
    
    where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # Overall summary
    query = f"""
        SELECT 
            COUNT(DISTINCT a.admission_id) as total_admissions,
            COUNT(DISTINCT CASE WHEN a.status = 'Discharged' THEN a.admission_id END) as total_discharges,
            AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
            SUM(b.total_amount) as total_revenue,
            AVG(b.total_amount) as avg_cost_per_patient,
            COUNT(DISTINCT pp.procedure_id) as total_procedures
        FROM admissions a
        LEFT JOIN billing b ON a.admission_id = b.admission_id
        LEFT JOIN patient_procedures pp ON a.admission_id = pp.admission_id
        {where_clause}
    """
    
    cursor.execute(query, params)
    summary = cursor.fetchone()
    
    # Convert Decimal to float
    if summary:
        if summary['avg_los']:
            summary['avg_los'] = float(summary['avg_los'])
        if summary['total_revenue']:
            summary['total_revenue'] = float(summary['total_revenue'])
        if summary['avg_cost_per_patient']:
            summary['avg_cost_per_patient'] = float(summary['avg_cost_per_patient'])
    
    # Department breakdown
    query = f"""
        SELECT 
            d.dept_name,
            COUNT(DISTINCT a.admission_id) as admissions,
            AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
            SUM(b.total_amount) as revenue
        FROM departments d
        LEFT JOIN admissions a ON d.dept_id = a.dept_id AND {where_conditions[0]}
        LEFT JOIN billing b ON a.admission_id = b.admission_id
        {'WHERE a.branch_id = %s' if branch_id else ''}
        GROUP BY d.dept_id, d.dept_name
        ORDER BY admissions DESC
    """
    
    cursor.execute(query, params if branch_id else [])
    dept_breakdown = cursor.fetchall()
    
    for row in dept_breakdown:
        if row['avg_los']:
            row['avg_los'] = float(row['avg_los'])
        if row['revenue']:
            row['revenue'] = float(row['revenue'])
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'month': month,
        'summary': summary,
        'department_breakdown': dept_breakdown,
        'generated_at': datetime.now().isoformat()
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """API health check endpoint"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500

# ============== RUN APPLICATION ==============

if __name__ == '__main__':
    print("="*60)
    print("Hospital Analytics Dashboard - Backend API")
    print("="*60)
    print("\nAvailable Endpoints:")
    print("  GET /api/kpis/summary - Overall KPI summary")
    print("  GET /api/trends/admissions - Admission trends")
    print("  GET /api/trends/bed-occupancy - Bed occupancy trends")
    print("  GET /api/departments/comparison - Department comparison")
    print("  GET /api/branches/comparison - Branch comparison")
    print("  GET /api/doctor-utilization - Doctor utilization stats")
    print("  GET /api/outcomes/summary - Patient outcomes")
    print("  GET /api/alerts/active - Active resource alerts")
    print("  GET /api/peak-hours - Peak admission hours/days")
    print("  GET /api/filters/options - Filter options")
    print("  GET /api/export/monthly-report - Monthly report data")
    print("  GET /api/health - Health check")
    print("\nStarting server on http://localhost:5000")
    print("="*60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)