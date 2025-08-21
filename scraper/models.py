"""
Data models for UK Premises Licence Scraper
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, HttpUrl
from enum import Enum


class LicenceType(str, Enum):
    """Types of premises licences"""
    NEW = "new"
    VARIATION = "variation"
    TRANSFER = "transfer"
    REVIEW = "review"
    PROVISIONAL = "provisional"


class LicenceStatus(str, Enum):
    """Status of licence applications"""
    GRANTED = "granted"
    PENDING = "pending"
    REFUSED = "refused"
    WITHDRAWN = "withdrawn"
    UNDER_REVIEW = "under_review"


class Council(BaseModel):
    """UK Council information"""
    name: str
    code: Optional[str] = None
    website_url: HttpUrl
    licence_register_url: Optional[HttpUrl] = None
    contact_email: Optional[str] = None
    region: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    last_scraped: Optional[datetime] = None
    scrape_successful: Optional[bool] = None
    error_message: Optional[str] = None


class PremisesLicence(BaseModel):
    """Premises licence data model"""
    # Core identification
    licence_id: str
    council_name: str
    council_code: Optional[str] = None
    
    # Premises information
    premises_name: str
    premises_address: str
    postcode: Optional[str] = None
    
    # Licence details
    licence_type: LicenceType
    licence_status: LicenceStatus
    application_date: Optional[datetime] = None
    granted_date: Optional[datetime] = None
    effective_date: Optional[datetime] = None
    
    # Activities and hours
    licensable_activities: List[str] = []
    opening_hours: Optional[Dict[str, str]] = None  # day -> hours
    alcohol_hours: Optional[Dict[str, str]] = None
    
    # Key personnel
    designated_premises_supervisor: Optional[str] = None
    dps_personal_licence_number: Optional[str] = None
    
    # Additional details
    conditions: List[str] = []
    variations: List[str] = []
    
    # Metadata
    source_url: Optional[HttpUrl] = None
    scraped_at: datetime = Field(default_factory=datetime.now)
    raw_data: Optional[Dict[str, Any]] = None


class WebsiteAnalysis(BaseModel):
    """Analysis of a council website structure"""
    council_name: str
    url: HttpUrl
    licence_register_found: bool = False
    potential_licence_urls: List[str] = []
    website_type: Optional[str] = None  # e.g., "custom", "gov.uk", "third_party"
    navigation_structure: Optional[Dict[str, Any]] = None
    search_functionality: bool = False
    pagination_detected: bool = False
    javascript_required: bool = False
    analysis_notes: Optional[str] = None
    analyzed_at: datetime = Field(default_factory=datetime.now)


class ScrapingResult(BaseModel):
    """Result of a scraping operation"""
    council_name: str
    success: bool
    licences_found: int = 0
    licences_extracted: List[PremisesLicence] = []
    error_message: Optional[str] = None
    scraping_duration: Optional[float] = None
    scraped_at: datetime = Field(default_factory=datetime.now)


class WeeklyReport(BaseModel):
    """Weekly report data"""
    report_date: datetime
    period_start: datetime
    period_end: datetime
    total_licences: int
    new_licences: int
    councils_scraped: int
    councils_successful: int
    summary_stats: Dict[str, Any]
    licences_by_type: Dict[LicenceType, int]
    licences_by_status: Dict[LicenceStatus, int]
    top_councils: List[Dict[str, Any]]
    error_summary: List[Dict[str, Any]]
