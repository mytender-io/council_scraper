"""
Data Processing Module

Cleans, validates, and structures premises licence data extracted from council websites.
Handles data quality, deduplication, geocoding, and enrichment.
"""

import json
import asyncio
import re
from typing import List, Dict, Optional, Tuple, Set, Any
from datetime import datetime, timedelta
import logging
from pathlib import Path
from collections import defaultdict, Counter

import pandas as pd
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

from .models import PremisesLicence, ScrapingResult, LicenceType, LicenceStatus
from .config import get_settings

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processes and cleans extracted premises licence data"""
    
    def __init__(self):
        self.settings = get_settings()
        self.llm = ChatOpenAI(
            model_name=self.settings.openai_model,
            api_key=self.settings.openai_api_key,
            temperature=0
        )
        
    def process_extraction_results(self, results: List[ScrapingResult]) -> List[PremisesLicence]:
        """Process all extraction results and return cleaned licences"""
        logger.info(f"Processing {len(results)} extraction results")
        
        # Collect all licences from results
        all_licences = []
        for result in results:
            if result.success and result.licences_extracted:
                all_licences.extend(result.licences_extracted)
        
        logger.info(f"Collected {len(all_licences)} raw licences")
        
        # Step 1: Clean individual licences
        cleaned_licences = self._clean_licences(all_licences)
        logger.info(f"Cleaned data: {len(cleaned_licences)} licences")
        
        # Step 2: Validate licences
        valid_licences = self._validate_licences(cleaned_licences)
        logger.info(f"Valid licences: {len(valid_licences)} licences")
        
        # Step 3: Deduplicate across councils
        unique_licences = self._deduplicate_licences(valid_licences)
        logger.info(f"Deduplicated: {len(unique_licences)} licences")
        
        # Step 4: Enrich data
        enriched_licences = self._enrich_licences(unique_licences)
        logger.info(f"Enriched: {len(enriched_licences)} licences")
        
        # Step 5: Sort by date (newest first)
        sorted_licences = sorted(
            enriched_licences,
            key=lambda x: x.granted_date or x.application_date or x.scraped_at,
            reverse=True
        )
        
        logger.info(f"Processing completed: {len(sorted_licences)} final licences")
        return sorted_licences
    
    def _clean_licences(self, licences: List[PremisesLicence]) -> List[PremisesLicence]:
        """Clean individual licence data"""
        cleaned = []
        
        for licence in licences:
            try:
                # Clean premises name
                licence.premises_name = self._clean_text(licence.premises_name)
                
                # Clean and standardize address
                licence.premises_address = self._clean_address(licence.premises_address)
                
                # Extract and validate postcode
                licence.postcode = self._extract_postcode(licence.premises_address)
                
                # Clean designated premises supervisor
                if licence.designated_premises_supervisor:
                    licence.designated_premises_supervisor = self._clean_person_name(
                        licence.designated_premises_supervisor
                    )
                
                # Clean and standardize activities
                licence.licensable_activities = self._clean_activities(licence.licensable_activities)
                
                # Clean conditions
                licence.conditions = [self._clean_text(condition) for condition in licence.conditions]
                licence.conditions = [c for c in licence.conditions if len(c) > 10]  # Remove very short conditions
                
                # Standardize opening hours
                if licence.opening_hours:
                    licence.opening_hours = self._standardize_hours(licence.opening_hours)
                    
                if licence.alcohol_hours:
                    licence.alcohol_hours = self._standardize_hours(licence.alcohol_hours)
                
                cleaned.append(licence)
                
            except Exception as e:
                logger.error(f"Error cleaning licence {licence.licence_id}: {e}")
                # Still include the licence but with original data
                cleaned.append(licence)
                
        return cleaned
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
            
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common HTML entities
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&nbsp;', ' ').replace('&quot;', '"').replace('&#39;', "'")
        
        # Remove control characters
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\t\n\r')
        
        return text.strip()
    
    def _clean_address(self, address: str) -> str:
        """Clean and standardize address format"""
        if not address:
            return ""
            
        address = self._clean_text(address)
        
        # Standardize common address abbreviations
        replacements = {
            r'\bSt\b': 'Street',
            r'\bRd\b': 'Road', 
            r'\bAve\b': 'Avenue',
            r'\bDr\b': 'Drive',
            r'\bCl\b': 'Close',
            r'\bLn\b': 'Lane',
            r'\bPl\b': 'Place',
            r'\bSq\b': 'Square',
            r'\bCt\b': 'Court',
        }
        
        for pattern, replacement in replacements.items():
            address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
        
        return address
    
    def _extract_postcode(self, address: str) -> Optional[str]:
        """Extract UK postcode from address"""
        if not address:
            return None
            
        # UK postcode regex patterns
        patterns = [
            r'[A-Z]{1,2}[0-9R][0-9A-Z]?\s*[0-9][A-Z]{2}',  # Standard format
            r'[A-Z]{2}[0-9]{1,2}\s*[0-9][A-Z]{2}',          # Alternative format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address.upper())
            if match:
                postcode = match.group().replace(' ', '')
                # Add space in correct position
                if len(postcode) >= 5:
                    return postcode[:-3] + ' ' + postcode[-3:]
                    
        return None
    
    def _clean_person_name(self, name: str) -> str:
        """Clean person name"""
        if not name:
            return ""
            
        name = self._clean_text(name)
        
        # Remove common titles and suffixes
        titles = ['mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'sir', 'lady', 'lord']
        suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v']
        
        words = name.split()
        filtered_words = []
        
        for word in words:
            word_lower = word.lower().rstrip('.,')
            if word_lower not in titles and word_lower not in suffixes:
                filtered_words.append(word)
                
        return ' '.join(filtered_words).title()
    
    def _clean_activities(self, activities: List[str]) -> List[str]:
        """Clean and standardize licensable activities"""
        if not activities:
            return []
            
        cleaned_activities = []
        
        # Standardize activity names
        activity_mapping = {
            'sale of alcohol': 'Sale of Alcohol',
            'supply of alcohol': 'Sale of Alcohol',
            'retail sale of alcohol': 'Sale of Alcohol',
            'live music': 'Live Music',
            'recorded music': 'Recorded Music',
            'performance of dance': 'Performance of Dance',
            'entertainment similar to music or dance': 'Entertainment Similar to Music or Dance',
            'late night refreshment': 'Late Night Refreshment',
            'provision of late night refreshment': 'Late Night Refreshment',
            'films': 'Exhibition of Films',
            'exhibition of films': 'Exhibition of Films',
            'indoor sporting events': 'Indoor Sporting Events',
            'boxing or wrestling': 'Boxing or Wrestling Entertainment',
        }
        
        for activity in activities:
            activity = self._clean_text(activity).lower()
            
            # Map to standard format
            standard_activity = None
            for key, value in activity_mapping.items():
                if key in activity:
                    standard_activity = value
                    break
                    
            if standard_activity:
                if standard_activity not in cleaned_activities:
                    cleaned_activities.append(standard_activity)
            elif len(activity) > 5:  # Keep non-standard activities if they're substantial
                cleaned_activities.append(activity.title())
                
        return cleaned_activities
    
    def _standardize_hours(self, hours: Dict[str, str]) -> Dict[str, str]:
        """Standardize opening hours format"""
        if not hours:
            return {}
            
        standardized = {}
        day_mapping = {
            'monday': 'Monday', 'mon': 'Monday',
            'tuesday': 'Tuesday', 'tue': 'Tuesday', 'tues': 'Tuesday',
            'wednesday': 'Wednesday', 'wed': 'Wednesday',
            'thursday': 'Thursday', 'thu': 'Thursday', 'thur': 'Thursday', 'thurs': 'Thursday',
            'friday': 'Friday', 'fri': 'Friday',
            'saturday': 'Saturday', 'sat': 'Saturday',
            'sunday': 'Sunday', 'sun': 'Sunday',
        }
        
        for day, hours_str in hours.items():
            # Standardize day name
            day_lower = day.lower().strip()
            standard_day = day_mapping.get(day_lower, day.title())
            
            # Clean hours string
            if hours_str:
                hours_clean = self._clean_text(hours_str)
                # Standardize time format (basic cleaning)
                hours_clean = re.sub(r'(\d{1,2}):(\d{2})', r'\1:\2', hours_clean)
                standardized[standard_day] = hours_clean
                
        return standardized
    
    def _validate_licences(self, licences: List[PremisesLicence]) -> List[PremisesLicence]:
        """Validate licence data and filter out invalid entries"""
        valid_licences = []
        
        for licence in licences:
            validation_errors = []
            
            # Check required fields
            if not licence.premises_name or len(licence.premises_name.strip()) < 3:
                validation_errors.append("Invalid premises name")
                
            if not licence.premises_address or len(licence.premises_address.strip()) < 10:
                validation_errors.append("Invalid premises address")
                
            if not licence.council_name:
                validation_errors.append("Missing council name")
                
            # Check dates are reasonable
            current_date = datetime.now()
            if licence.application_date and licence.application_date > current_date + timedelta(days=30):
                validation_errors.append("Invalid application date (too far in future)")
                
            if licence.granted_date and licence.granted_date > current_date + timedelta(days=30):
                validation_errors.append("Invalid granted date (too far in future)")
                
            # Check for obviously invalid data
            if any(word in licence.premises_name.lower() for word in [
                'loading', 'error', 'not found', 'search', 'menu', 'navigation'
            ]):
                validation_errors.append("Premises name appears to be navigation/error text")
                
            if validation_errors:
                logger.debug(f"Licence {licence.licence_id} validation failed: {validation_errors}")
            else:
                valid_licences.append(licence)
                
        logger.info(f"Validation: {len(valid_licences)}/{len(licences)} licences passed")
        return valid_licences
    
    def _deduplicate_licences(self, licences: List[PremisesLicence]) -> List[PremisesLicence]:
        """Advanced deduplication across councils"""
        if not licences:
            return []
            
        # Group by normalized premises name and address
        groups = defaultdict(list)
        
        for licence in licences:
            # Create normalized key for grouping
            name_key = self._normalize_for_matching(licence.premises_name)
            address_key = self._normalize_for_matching(licence.premises_address)
            
            # Use postcode if available for better matching
            if licence.postcode:
                key = f"{name_key}|{licence.postcode}"
            else:
                key = f"{name_key}|{address_key}"
                
            groups[key].append(licence)
        
        unique_licences = []
        
        for key, group in groups.items():
            if len(group) == 1:
                unique_licences.append(group[0])
            else:
                # Multiple licences for same premises - pick the best one
                best_licence = self._select_best_licence(group)
                unique_licences.append(best_licence)
                logger.debug(f"Deduplicated {len(group)} licences for {key}")
        
        logger.info(f"Deduplication: {len(unique_licences)} unique licences from {len(licences)}")
        return unique_licences
    
    def _normalize_for_matching(self, text: str) -> str:
        """Normalize text for matching purposes"""
        if not text:
            return ""
            
        # Convert to lowercase and remove common noise
        normalized = text.lower()
        
        # Remove common business suffixes/prefixes
        business_words = [
            'the ', ' ltd', ' limited', ' plc', ' pub', ' restaurant', ' bar',
            ' hotel', ' inn', ' tavern', ' club', ' cafe', ' coffee', ' shop'
        ]
        
        for word in business_words:
            normalized = normalized.replace(word, '')
            
        # Remove punctuation and extra spaces
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _select_best_licence(self, licences: List[PremisesLicence]) -> PremisesLicence:
        """Select the best licence from a group of duplicates"""
        # Score each licence based on data completeness
        def score_licence(licence):
            score = 0
            
            # More recent data is better
            if licence.granted_date:
                score += 10
            if licence.application_date:
                score += 5
                
            # More complete data is better
            if licence.postcode:
                score += 3
            if licence.designated_premises_supervisor:
                score += 2
            if licence.licensable_activities:
                score += len(licence.licensable_activities)
            if licence.conditions:
                score += len(licence.conditions) // 2  # Don't over-weight conditions
            if licence.opening_hours:
                score += 2
                
            # Prefer granted over pending
            if licence.licence_status == LicenceStatus.GRANTED:
                score += 5
                
            return score
        
        # Return the highest scoring licence
        return max(licences, key=score_licence)
    
    def _enrich_licences(self, licences: List[PremisesLicence]) -> List[PremisesLicence]:
        """Enrich licence data with additional information"""
        logger.info("Enriching licence data")
        
        for licence in licences:
            try:
                # Add business type classification
                licence.business_type = self._classify_business_type(licence)
                
                # Standardize licence categories
                licence.licence_categories = self._categorize_licence(licence)
                
                # Add risk score (basic implementation)
                licence.risk_score = self._calculate_risk_score(licence)
                
            except Exception as e:
                logger.error(f"Error enriching licence {licence.licence_id}: {e}")
                
        return licences
    
    def _classify_business_type(self, licence: PremisesLicence) -> str:
        """Classify business type based on premises name and activities"""
        name_lower = licence.premises_name.lower()
        activities = [a.lower() for a in licence.licensable_activities]
        
        # Business type classifications
        if any(word in name_lower for word in ['pub', 'tavern', 'inn']):
            return 'Pub'
        elif any(word in name_lower for word in ['restaurant', 'bistro', 'brasserie', 'dining']):
            return 'Restaurant'
        elif any(word in name_lower for word in ['bar', 'cocktail', 'wine bar']):
            return 'Bar'
        elif any(word in name_lower for word in ['hotel']):
            return 'Hotel'
        elif any(word in name_lower for word in ['club', 'nightclub']):
            return 'Club'
        elif any(word in name_lower for word in ['cafe', 'coffee']):
            return 'Cafe'
        elif any(word in name_lower for word in ['shop', 'store', 'market', 'supermarket']):
            return 'Retail'
        elif any(word in name_lower for word in ['theatre', 'cinema', 'venue']):
            return 'Entertainment Venue'
        elif 'late night refreshment' in activities and 'sale of alcohol' not in activities:
            return 'Takeaway'
        else:
            return 'Other'
    
    def _categorize_licence(self, licence: PremisesLicence) -> List[str]:
        """Categorize licence based on activities"""
        categories = []
        activities = [a.lower() for a in licence.licensable_activities]
        
        if any('alcohol' in activity for activity in activities):
            categories.append('Alcohol')
            
        if any('music' in activity or 'dance' in activity for activity in activities):
            categories.append('Entertainment')
            
        if any('refreshment' in activity for activity in activities):
            categories.append('Late Night Refreshment')
            
        if any('film' in activity for activity in activities):
            categories.append('Films')
            
        if any('sport' in activity for activity in activities):
            categories.append('Sports')
            
        return categories or ['General']
    
    def _calculate_risk_score(self, licence: PremisesLicence) -> int:
        """Calculate a basic risk score (1-10) for the premises"""
        score = 5  # Base score
        
        # Factors that increase risk
        activities = [a.lower() for a in licence.licensable_activities]
        
        if 'late night refreshment' in activities:
            score += 1
        if any('music' in activity or 'dance' in activity for activity in activities):
            score += 1
        if licence.business_type in ['Club', 'Bar']:
            score += 2
        if licence.business_type == 'Takeaway':
            score += 1
            
        # Factors that decrease risk  
        if licence.business_type in ['Hotel', 'Restaurant']:
            score -= 1
        if len(licence.conditions) > 5:  # More conditions = lower risk
            score -= 1
            
        return max(1, min(10, score))  # Clamp to 1-10 range
    
    def generate_processing_summary(self, original_count: int, final_licences: List[PremisesLicence]) -> Dict[str, Any]:
        """Generate summary of data processing"""
        summary = {
            'processing_date': datetime.now().isoformat(),
            'original_count': original_count,
            'final_count': len(final_licences),
            'reduction_rate': (original_count - len(final_licences)) / original_count if original_count > 0 else 0,
        }
        
        # Council breakdown
        council_counts = Counter(licence.council_name for licence in final_licences)
        summary['councils'] = dict(council_counts.most_common())
        
        # Business type breakdown
        business_type_counts = Counter(getattr(licence, 'business_type', 'Unknown') for licence in final_licences)
        summary['business_types'] = dict(business_type_counts)
        
        # Licence status breakdown
        status_counts = Counter(licence.licence_status.value for licence in final_licences)
        summary['licence_statuses'] = dict(status_counts)
        
        # Date range
        dates = [licence.granted_date or licence.application_date for licence in final_licences if licence.granted_date or licence.application_date]
        if dates:
            summary['date_range'] = {
                'earliest': min(dates).isoformat(),
                'latest': max(dates).isoformat(),
                'span_days': (max(dates) - min(dates)).days
            }
        
        # Activity breakdown
        all_activities = []
        for licence in final_licences:
            all_activities.extend(licence.licensable_activities)
        activity_counts = Counter(all_activities)
        summary['top_activities'] = dict(activity_counts.most_common(10))
        
        return summary
    
    def save_processed_data(self, licences: List[PremisesLicence], summary: Dict[str, Any], filename: Optional[str] = None):
        """Save processed licence data and summary"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.settings.data_dir}/licences/processed_licences_{timestamp}.json"
            
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'summary': summary,
            'licences': [licence.dict() for licence in licences]
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
            
        logger.info(f"Saved {len(licences)} processed licences to {filename}")
        
        # Also save summary separately
        summary_filename = filename.replace('.json', '_summary.json')
        with open(summary_filename, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str, ensure_ascii=False)
            
        return filename
    
    def load_processed_data(self, filename: str) -> Tuple[List[PremisesLicence], Dict[str, Any]]:
        """Load processed licence data"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            summary = data.get('summary', {})
            licences_data = data.get('licences', [])
            
            licences = [PremisesLicence(**licence_data) for licence_data in licences_data]
            
            logger.info(f"Loaded {len(licences)} processed licences from {filename}")
            return licences, summary
            
        except FileNotFoundError:
            logger.warning(f"No processed data found at {filename}")
            return [], {}


# CLI function for standalone usage
async def main():
    """Main function for running data processing standalone"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from .data_extractor import DataExtractor
    
    # Load latest extraction results
    data_dir = Path("data/licences")
    if not data_dir.exists():
        print("No extraction results found. Run data extraction first.")
        return
        
    # Find most recent extraction results file
    result_files = list(data_dir.glob("extraction_results_*.json"))
    if not result_files:
        print("No extraction result files found.")
        return
        
    latest_file = max(result_files, key=lambda f: f.stat().st_mtime)
    print(f"Loading extraction results from: {latest_file}")
    
    # Load results
    extractor = DataExtractor()
    results = extractor.load_extraction_results(str(latest_file))
    
    if not results:
        print("No results loaded.")
        return
        
    print(f"Loaded {len(results)} extraction results")
    
    # Process the data
    processor = DataProcessor()
    
    original_count = sum(len(r.licences_extracted) for r in results if r.licences_extracted)
    processed_licences = processor.process_extraction_results(results)
    
    # Generate summary
    summary = processor.generate_processing_summary(original_count, processed_licences)
    
    # Save processed data
    filename = processor.save_processed_data(processed_licences, summary)
    
    print(f"\nProcessing completed:")
    print(f"Original licences: {original_count}")
    print(f"Final licences: {len(processed_licences)}")
    print(f"Reduction: {summary['reduction_rate']:.1%}")
    print(f"Saved to: {filename}")


if __name__ == "__main__":
    asyncio.run(main())
