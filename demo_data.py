"""
Create sample demonstration data for the UK Premises Licence Scraper
"""

import json
from datetime import datetime, timedelta
from scraper.models import PremisesLicence, LicenceType, LicenceStatus
from scraper.data_processor import DataProcessor
from scraper.report_generator import ReportGenerator

# Create sample premises licence data
sample_licences = [
    PremisesLicence(
        licence_id="WCC_001",
        council_name="Westminster City Council",
        council_code="WCC",
        premises_name="The Red Lion",
        premises_address="123 Oxford Street, London",
        postcode="W1D 2HT",
        licence_type=LicenceType.NEW,
        licence_status=LicenceStatus.GRANTED,
        application_date=datetime.now() - timedelta(days=30),
        granted_date=datetime.now() - timedelta(days=15),
        licensable_activities=["Sale of Alcohol", "Live Music", "Recorded Music"],
        opening_hours={"Monday": "11:00-23:00", "Friday": "11:00-24:00", "Saturday": "11:00-24:00"},
        designated_premises_supervisor="John Smith",
        conditions=["No music after 23:00", "Maximum capacity 100 people"],
        source_url="https://www.westminster.gov.uk/licensing"
    ),
    PremisesLicence(
        licence_id="ISL_001", 
        council_name="Islington Council",
        council_code="ISL",
        premises_name="Pizza Express",
        premises_address="456 Upper Street, London",
        postcode="N1 0QY",
        licence_type=LicenceType.NEW,
        licence_status=LicenceStatus.GRANTED,
        application_date=datetime.now() - timedelta(days=45),
        granted_date=datetime.now() - timedelta(days=20),
        licensable_activities=["Sale of Alcohol", "Late Night Refreshment"],
        opening_hours={"Monday": "12:00-23:30", "Sunday": "12:00-22:30"},
        designated_premises_supervisor="Maria Rossi",
        conditions=["No deliveries before 8:00 AM"],
        source_url="https://www.islington.gov.uk/business/licensing"
    ),
    PremisesLicence(
        licence_id="MAN_001",
        council_name="Manchester City Council", 
        council_code="MCC",
        premises_name="The Northern Quarter Club",
        premises_address="789 Oldham Street, Manchester",
        postcode="M1 1JQ",
        licence_type=LicenceType.NEW,
        licence_status=LicenceStatus.GRANTED,
        application_date=datetime.now() - timedelta(days=60),
        granted_date=datetime.now() - timedelta(days=35),
        licensable_activities=["Sale of Alcohol", "Live Music", "Recorded Music", "Performance of Dance"],
        opening_hours={"Friday": "20:00-03:00", "Saturday": "20:00-03:00"},
        designated_premises_supervisor="David Williams",
        conditions=["Sound limiter installed", "No entry after 1:00 AM", "SIA licensed door staff required"],
        source_url="https://www.manchester.gov.uk/licensing"
    ),
    PremisesLicence(
        licence_id="BRI_001",
        council_name="Bristol City Council",
        council_code="BRI",
        premises_name="Harbour View Restaurant",
        premises_address="12 The Harbourside, Bristol",
        postcode="BS1 5DB",
        licence_type=LicenceType.VARIATION,
        licence_status=LicenceStatus.PENDING,
        application_date=datetime.now() - timedelta(days=10),
        granted_date=None,
        licensable_activities=["Sale of Alcohol", "Live Music"],
        opening_hours={"Monday": "11:00-22:00", "Sunday": "12:00-21:00"},
        designated_premises_supervisor="Sarah Johnson", 
        conditions=["Alcohol served with substantial meals only"],
        source_url="https://www.bristol.gov.uk/licensing"
    ),
    PremisesLicence(
        licence_id="LED_001",
        council_name="Leeds City Council",
        council_code="LCC", 
        premises_name="Tesco Express",
        premises_address="567 Headingley Lane, Leeds",
        postcode="LS6 3AA",
        licence_type=LicenceType.NEW,
        licence_status=LicenceStatus.GRANTED,
        application_date=datetime.now() - timedelta(days=25),
        granted_date=datetime.now() - timedelta(days=5),
        licensable_activities=["Sale of Alcohol"],
        opening_hours={"Monday": "06:00-23:00", "Sunday": "10:00-22:00"},
        designated_premises_supervisor="Ahmed Patel",
        conditions=["No single cans of beer or cider", "Challenge 25 policy"],
        source_url="https://www.leeds.gov.uk/licensing"
    )
]

if __name__ == "__main__":
    print("Creating demonstration data and reports...")
    
    # Process the sample data
    processor = DataProcessor()
    
    # Enrich the sample data
    for licence in sample_licences:
        licence.business_type = processor._classify_business_type(licence)
        licence.licence_categories = processor._categorize_licence(licence)
        licence.risk_score = processor._calculate_risk_score(licence)
    
    # Generate summary
    summary = processor.generate_processing_summary(len(sample_licences), sample_licences)
    
    # Save processed data
    filename = processor.save_processed_data(sample_licences, summary)
    print(f"Sample data saved to: {filename}")
    
    # Generate reports
    generator = ReportGenerator()
    
    # Weekly report
    weekly_report = generator.generate_weekly_report(sample_licences)
    print(f"Demo weekly report: {weekly_report}")
    
    # Full dataset report  
    full_report = generator.generate_full_dataset_report(sample_licences)
    print(f"Demo full report: {full_report}")
    
    print("\nDemonstration complete! Check the reports/ directory for Excel files.")
    print(f"Total sample licences: {len(sample_licences)}")
    print(f"Business types: {[getattr(l, 'business_type', 'Unknown') for l in sample_licences]}")
    print(f"Risk scores: {[getattr(l, 'risk_score', 5) for l in sample_licences]}")
