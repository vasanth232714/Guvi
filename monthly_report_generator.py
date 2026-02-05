"""
Automated Monthly Report Generator for Hospital Analytics
Generates comprehensive PDF and CSV reports for specified month
"""

import mysql.connector
from datetime import datetime, timedelta
import csv
import json
from decimal import Decimal

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'hospital_analytics',
    'user': 'hospital_admin',
    'password': 'SecurePassword123!'  # Update with your MySQL password
}

def get_db_connection():
    """Create database connection"""
    return mysql.connector.connect(**DB_CONFIG)

def decimal_to_float(obj):
    """Convert Decimal to float"""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

class MonthlyReportGenerator:
    def __init__(self, year, month, branch_id=None):
        self.year = year
        self.month = month
        self.branch_id = branch_id
        self.month_str = f"{year}-{month:02d}"
        self.connection = get_db_connection()
        self.cursor = self.connection.cursor(dictionary=True)
        
    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'connection'):
            self.connection.close()
    
    def get_summary_metrics(self):
        """Get overall summary metrics for the month"""
        where_clause = f"WHERE DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND a.branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                COUNT(DISTINCT a.admission_id) as total_admissions,
                COUNT(DISTINCT CASE WHEN a.status = 'Discharged' THEN a.admission_id END) as total_discharges,
                COUNT(DISTINCT CASE WHEN a.admission_type = 'Emergency' THEN a.admission_id END) as emergency_admissions,
                AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
                SUM(b.total_amount) as total_revenue,
                AVG(b.total_amount) as avg_cost_per_patient,
                COUNT(DISTINCT pp.procedure_id) as total_procedures,
                COUNT(DISTINCT CASE WHEN o.readmission_within_30days = TRUE THEN a.admission_id END) as readmissions
            FROM admissions a
            LEFT JOIN billing b ON a.admission_id = b.admission_id
            LEFT JOIN patient_procedures pp ON a.admission_id = pp.admission_id
            LEFT JOIN outcomes o ON a.admission_id = o.admission_id
            {where_clause}
        """
        
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        
        # Convert Decimal to float
        for key in result:
            result[key] = decimal_to_float(result[key])
        
        # Calculate readmission rate
        if result['total_discharges'] and result['total_discharges'] > 0:
            result['readmission_rate'] = (result['readmissions'] / result['total_discharges']) * 100
        else:
            result['readmission_rate'] = 0
        
        # Calculate emergency percentage
        if result['total_admissions'] and result['total_admissions'] > 0:
            result['emergency_percentage'] = (result['emergency_admissions'] / result['total_admissions']) * 100
        else:
            result['emergency_percentage'] = 0
        
        return result
    
    def get_department_breakdown(self):
        """Get department-wise breakdown"""
        where_clause = f"WHERE DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND d.branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                d.dept_name,
                d.dept_type,
                COUNT(DISTINCT a.admission_id) as admissions,
                COUNT(DISTINCT CASE WHEN a.status = 'Discharged' THEN a.admission_id END) as discharges,
                AVG(DATEDIFF(COALESCE(a.discharge_date, CURRENT_DATE), a.admission_date)) as avg_los,
                SUM(b.total_amount) as revenue,
                AVG(b.total_amount) as avg_revenue_per_patient,
                COUNT(DISTINCT pp.procedure_id) as procedures,
                COUNT(DISTINCT CASE WHEN a.admission_type = 'Emergency' THEN a.admission_id END) as emergency_cases
            FROM departments d
            LEFT JOIN admissions a ON d.dept_id = a.dept_id AND DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'
            LEFT JOIN billing b ON a.admission_id = b.admission_id
            LEFT JOIN patient_procedures pp ON a.admission_id = pp.admission_id
            {f'WHERE d.branch_id = {self.branch_id}' if self.branch_id else ''}
            GROUP BY d.dept_id, d.dept_name, d.dept_type
            HAVING admissions > 0
            ORDER BY revenue DESC
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        # Convert Decimal to float
        for row in results:
            for key in row:
                row[key] = decimal_to_float(row[key])
        
        return results
    
    def get_bed_occupancy_stats(self):
        """Get bed occupancy statistics"""
        where_clause = f"WHERE DATE_FORMAT(snapshot_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                AVG(occupancy_rate) as avg_occupancy,
                MAX(occupancy_rate) as max_occupancy,
                MIN(occupancy_rate) as min_occupancy,
                AVG(icu_occupied) as avg_icu_occupied,
                AVG(general_occupied) as avg_general_occupied
            FROM bed_occupancy_daily
            {where_clause} AND dept_id IS NULL
        """
        
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        
        # Convert Decimal to float
        for key in result:
            result[key] = decimal_to_float(result[key])
        
        return result
    
    def get_doctor_performance(self):
        """Get doctor performance metrics"""
        where_clause = f"WHERE DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND doc.branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                doc.doctor_name,
                dep.dept_name,
                COUNT(DISTINCT a.admission_id) as patients_handled,
                COUNT(DISTINCT pp.procedure_id) as procedures_performed,
                AVG(pp.duration_minutes) as avg_procedure_duration,
                SUM(b.total_amount) as revenue_generated
            FROM doctors doc
            JOIN departments dep ON doc.dept_id = dep.dept_id
            LEFT JOIN admissions a ON doc.doctor_id = a.doctor_id 
                AND DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'
            LEFT JOIN patient_procedures pp ON doc.doctor_id = pp.doctor_id
                AND DATE_FORMAT(pp.procedure_date, '%Y-%m') = '{self.month_str}'
            LEFT JOIN billing b ON a.admission_id = b.admission_id
            {'WHERE doc.branch_id = ' + str(self.branch_id) if self.branch_id else ''}
            GROUP BY doc.doctor_id, doc.doctor_name, dep.dept_name
            HAVING patients_handled > 0
            ORDER BY patients_handled DESC
            LIMIT 20
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        # Convert Decimal to float
        for row in results:
            for key in row:
                row[key] = decimal_to_float(row[key])
        
        return results
    
    def get_patient_outcomes(self):
        """Get patient outcome distribution"""
        where_clause = f"WHERE DATE_FORMAT(o.outcome_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND a.branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                o.outcome_type,
                COUNT(*) as count,
                AVG(DATEDIFF(a.discharge_date, a.admission_date)) as avg_los_for_outcome
            FROM outcomes o
            JOIN admissions a ON o.admission_id = a.admission_id
            {where_clause}
            GROUP BY o.outcome_type
            ORDER BY count DESC
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        # Convert Decimal to float
        for row in results:
            for key in row:
                row[key] = decimal_to_float(row[key])
        
        return results
    
    def get_revenue_breakdown(self):
        """Get revenue breakdown by category"""
        where_clause = f"WHERE DATE_FORMAT(a.admission_date, '%Y-%m') = '{self.month_str}'"
        if self.branch_id:
            where_clause += f" AND a.branch_id = {self.branch_id}"
        
        query = f"""
            SELECT 
                SUM(b.room_charges) as room_charges,
                SUM(b.procedure_charges) as procedure_charges,
                SUM(b.medicine_charges) as medicine_charges,
                SUM(b.lab_charges) as lab_charges,
                SUM(b.other_charges) as other_charges,
                SUM(b.total_amount) as total_revenue,
                SUM(b.discount) as total_discount,
                SUM(b.insurance_coverage) as insurance_coverage,
                SUM(b.amount_paid) as total_collected
            FROM billing b
            JOIN admissions a ON b.admission_id = a.admission_id
            {where_clause}
        """
        
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        
        # Convert Decimal to float
        for key in result:
            result[key] = decimal_to_float(result[key])
        
        # Calculate collection rate
        if result['total_revenue']:
            result['collection_rate'] = (result['total_collected'] / result['total_revenue']) * 100
        else:
            result['collection_rate'] = 0
        
        return result
    
    def generate_text_report(self):
        """Generate text-based report"""
        report = []
        report.append("="*80)
        report.append(f"HOSPITAL MONTHLY PERFORMANCE REPORT")
        report.append(f"Period: {datetime(self.year, self.month, 1).strftime('%B %Y')}")
        if self.branch_id:
            # Get branch name
            self.cursor.execute(f"SELECT branch_name FROM branches WHERE branch_id = {self.branch_id}")
            branch = self.cursor.fetchone()
            report.append(f"Branch: {branch['branch_name']}")
        else:
            report.append("Branch: All Branches")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("="*80)
        report.append("")
        
        # Summary Metrics
        summary = self.get_summary_metrics()
        report.append("SUMMARY METRICS")
        report.append("-"*80)
        report.append(f"Total Admissions:          {summary['total_admissions']:>10}")
        report.append(f"Total Discharges:          {summary['total_discharges']:>10}")
        report.append(f"Emergency Admissions:      {summary['emergency_admissions']:>10} ({summary['emergency_percentage']:.1f}%)")
        avg_los = summary['avg_los']
        if avg_los is None:
            report.append("Average Length of Stay:        N/A")
        else:
            report.append(f"Average Length of Stay:    {avg_los:>10.2f} days")

        report.append(f"Total Procedures:          {summary['total_procedures']:>10}")
        report.append(f"Readmission Rate:          {summary['readmission_rate']:>10.2f}%")
        total_revenue = summary['total_revenue']
        if total_revenue is None:
            report.append("Total Revenue:             N/A")
        else:
            report.append(f"Total Revenue:             ₹{total_revenue:>10,.2f}")

        avg_cost = summary['avg_cost_per_patient']
        if avg_cost is None:
            report.append("Avg Cost per Patient:      N/A")
        else:
            report.append(f"Avg Cost per Patient:      ₹{avg_cost:>10,.2f}")

        report.append("")
        
        # Bed Occupancy
        occupancy = self.get_bed_occupancy_stats()
        report.append("BED OCCUPANCY STATISTICS")
        report.append("-"*80)
        def fmt_float(value, suffix=""):
            return "N/A" if value is None else f"{value:>10.2f}{suffix}"

        report.append(f"Average Occupancy Rate:    {fmt_float(occupancy['avg_occupancy'], '%')}")
        report.append(f"Maximum Occupancy Rate:    {fmt_float(occupancy['max_occupancy'], '%')}")
        report.append(f"Minimum Occupancy Rate:    {fmt_float(occupancy['min_occupancy'], '%')}")
        report.append(f"Avg ICU Beds Occupied:     {fmt_float(occupancy['avg_icu_occupied'])}")
        report.append(f"Avg General Beds Occupied: {fmt_float(occupancy['avg_general_occupied'])}")
        report.append("")

        # Department Breakdown
        departments = self.get_department_breakdown()
        report.append("DEPARTMENT PERFORMANCE")
        report.append("-"*80)
        report.append(f"{'Department':<20} {'Admits':>8} {'Dischar':>8} {'Avg LOS':>8} {'Revenue':>15} {'Procedures':>10}")
        report.append("-"*80)
        for dept in departments:
            report.append(
                f"{dept['dept_name']:<20} "
                f"{dept['admissions']:>8} "
                f"{dept['discharges']:>8} "
                f"{dept['avg_los']:>8.2f} "
                f"₹{dept['revenue']:>13,.0f} "
                f"{dept['procedures']:>10}"
            )
        report.append("")
        
        # Revenue Breakdown
        revenue = self.get_revenue_breakdown()

        report.append("REVENUE BREAKDOWN")
        report.append("-" * 80)
        def fmt_money(value):
             return "N/A".rjust(15) if value is None else f"₹{value:>15,.2f}"

        def fmt_percent(value):
            return "N/A".rjust(15) if value is None else f"{value:>15.2f}%"

        report.append(f"Room Charges:              {fmt_money(revenue['room_charges'])}")
        report.append(f"Procedure Charges:         {fmt_money(revenue['procedure_charges'])}")
        report.append(f"Medicine Charges:          {fmt_money(revenue['medicine_charges'])}")
        report.append(f"Lab Charges:               {fmt_money(revenue['lab_charges'])}")
        report.append(f"Other Charges:             {fmt_money(revenue['other_charges'])}")
        report.append(f"Total Revenue:             {fmt_money(revenue['total_revenue'])}")
        report.append(f"Total Discount:            {fmt_money(revenue['total_discount'])}")
        report.append(f"Insurance Coverage:        {fmt_money(revenue['insurance_coverage'])}")
        report.append(f"Total Collected:           {fmt_money(revenue['total_collected'])}")
        report.append(f"Collection Rate:           {fmt_percent(revenue['collection_rate'])}")

        report.append("")

        # Patient Outcomes
        outcomes = self.get_patient_outcomes()
        report.append("PATIENT OUTCOMES")
        report.append("-"*80)
        report.append(f"{'Outcome Type':<20} {'Count':>10} {'Avg LOS':>12}")
        report.append("-"*80)
        for outcome in outcomes:
            report.append(
                f"{outcome['outcome_type']:<20} "
                f"{outcome['count']:>10} "
                f"{outcome['avg_los_for_outcome']:>12.2f}"
            )
        report.append("")
        
        # Top Doctors
        doctors = self.get_doctor_performance()
        report.append("TOP 20 DOCTORS BY PATIENT VOLUME")
        report.append("-"*80)
        report.append(f"{'Doctor Name':<25} {'Department':<20} {'Patients':>8} {'Procedures':>10} {'Revenue':>15}")
        report.append("-"*80)
        for doctor in doctors:
            report.append(
                f"{doctor['doctor_name']:<25} "
                f"{doctor['dept_name']:<20} "
                f"{doctor['patients_handled']:>8} "
                f"{doctor['procedures_performed']:>10} "
                f"₹{doctor['revenue_generated']:>13,.0f}"
            )
        report.append("")
        report.append("="*80)
        report.append("END OF REPORT")
        report.append("="*80)
        
        return "\n".join(report)
    
    def export_to_csv(self, filename):
        """Export department data to CSV"""
        departments = self.get_department_breakdown()
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['dept_name', 'admissions', 'discharges', 'avg_los', 'revenue', 
                         'avg_revenue_per_patient', 'procedures', 'emergency_cases']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for dept in departments:
                writer.writerow(dept)
        
        print(f"CSV exported to: {filename}")
    
    def export_to_json(self, filename):
        """Export all data to JSON"""
        data = {
            'report_period': self.month_str,
            'generated_at': datetime.now().isoformat(),
            'summary': self.get_summary_metrics(),
            'bed_occupancy': self.get_bed_occupancy_stats(),
            'departments': self.get_department_breakdown(),
            'revenue': self.get_revenue_breakdown(),
            'outcomes': self.get_patient_outcomes(),
            'top_doctors': self.get_doctor_performance()
        }
        with open(filename, 'w') as jsonfile:
            json.dump(data, jsonfile, indent=2, default=str)
        print(f"JSON exported to: {filename}")

def main():
    """Main execution function"""
    print("="*80)
    print("HOSPITAL MONTHLY REPORT GENERATOR")
    print("="*80)
    print()
    # Get report parameters
    year = int(input("Enter year (e.g., 2024): "))
    month = int(input("Enter month (1-12): "))
    branch_input = input("Enter branch ID (press Enter for all branches): ")
    branch_id = int(branch_input) if branch_input else None
    print("\nGenerating report...")
    print()
    # Generate report
    generator = MonthlyReportGenerator(year, month, branch_id)
    # Generate text report
    text_report = generator.generate_text_report()
    # Save to file
    filename_base = f"hospital_report_{year}_{month:02d}"
    if branch_id:
        filename_base += f"_branch{branch_id}"
    txt_filename = f"{filename_base}.txt"
    csv_filename = f"{filename_base}_departments.csv"
    json_filename = f"{filename_base}_data.json"
    # Save text report
    with open(txt_filename, 'w') as f:
        f.write(text_report)
    print(f"Text report saved to: {txt_filename}")
    # Export CSV
    generator.export_to_csv(csv_filename)
    # Export JSON
    generator.export_to_json(json_filename)

    print()
    print("Report generation complete!")
    print()
    print("Preview:")
    print()
    print(text_report[:1500])  # Show first 1500 characters
    print("\n... (see full report in file)")

if __name__ == "__main__":
    main()