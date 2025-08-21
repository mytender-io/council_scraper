"""
Main Orchestrator Module

Coordinates all components of the UK Premises Licence Scraper.
Provides main entry points and workflow management.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import json

from .config import get_settings, setup_directories
from .models import Council, WebsiteAnalysis, PremisesLicence, ScrapingResult
from .council_discovery import CouncilDiscovery
from .website_analyzer import WebsiteAnalyzer
from .data_extractor import DataExtractor
from .data_processor import DataProcessor
from .report_generator import ReportGenerator

logger = logging.getLogger(__name__)


class PremisesLicenceScraper:
    """Main orchestrator for the UK Premises Licence Scraper"""
    
    def __init__(self):
        self.settings = get_settings()
        setup_directories()
        
        # Initialize components
        self.council_discovery = CouncilDiscovery()
        self.website_analyzer = WebsiteAnalyzer()
        self.data_extractor = DataExtractor()
        self.data_processor = DataProcessor()
        self.report_generator = ReportGenerator()
        
        # State tracking
        self.councils: List[Council] = []
        self.analyses: List[WebsiteAnalysis] = []
        self.extraction_results: List[ScrapingResult] = []
        self.processed_licences: List[PremisesLicence] = []
        
    async def run_full_scrape(self, max_councils: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete scraping workflow"""
        logger.info("Starting full premises licence scrape")
        start_time = datetime.now()
        
        try:
            # Step 1: Discover councils
            await self._step_discover_councils()
            
            # Step 2: Analyze websites
            await self._step_analyze_websites()
            
            # Step 3: Extract licence data
            await self._step_extract_data(max_councils)
            
            # Step 4: Process and clean data
            await self._step_process_data()
            
            # Step 5: Generate reports
            await self._step_generate_reports()
            
            # Generate summary
            summary = self._generate_run_summary(start_time)
            
            logger.info("Full scrape completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Full scrape failed: {e}")
            raise
    
    async def run_incremental_update(self, days_back: int = 7) -> Dict[str, Any]:
        """Run incremental update for recent data"""
        logger.info(f"Starting incremental update for last {days_back} days")
        start_time = datetime.now()
        
        try:
            # Load existing councils and analyses
            self.councils = self.council_discovery.load_councils_data()
            self.analyses = self.website_analyzer.load_analyses()
            
            if not self.councils:
                logger.info("No existing councils found, running full discovery")
                await self._step_discover_councils()
                await self._step_analyze_websites()
            
            # Filter to councils that have been successful recently
            recent_councils = [
                c for c in self.councils 
                if c.last_scraped and c.last_scraped > datetime.now() - timedelta(days=30) and c.scrape_successful
            ]
            
            if not recent_councils:
                logger.warning("No recently successful councils found, falling back to all councils")
                recent_councils = [c for c in self.councils if c.licence_register_url and c.scrape_successful]
            
            logger.info(f"Running incremental update on {len(recent_councils)} councils")
            
            # Extract data from recent councils only
            self.extraction_results = await self.data_extractor.extract_all_licences(
                recent_councils[:50] if len(recent_councils) > 50 else recent_councils,  # Limit for incremental
                self.analyses
            )
            
            # Process data
            await self._step_process_data()
            
            # Generate reports
            await self._step_generate_reports()
            
            summary = self._generate_run_summary(start_time, incremental=True)
            
            logger.info("Incremental update completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            raise
    
    async def run_council_discovery_only(self) -> Dict[str, Any]:
        """Run only council discovery and website analysis"""
        logger.info("Running council discovery and website analysis only")
        start_time = datetime.now()
        
        try:
            await self._step_discover_councils()
            await self._step_analyze_websites()
            
            summary = {
                'step': 'discovery_only',
                'start_time': start_time.isoformat(),
                'duration': (datetime.now() - start_time).total_seconds(),
                'councils_discovered': len(self.councils),
                'councils_with_registers': len([c for c in self.councils if c.licence_register_url]),
                'successful_analyses': len([a for a in self.analyses if a.licence_register_found])
            }
            
            logger.info("Council discovery completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Council discovery failed: {e}")
            raise
    
    async def run_data_extraction_only(self, max_councils: Optional[int] = None) -> Dict[str, Any]:
        """Run only data extraction from existing council data"""
        logger.info("Running data extraction only")
        start_time = datetime.now()
        
        try:
            # Load existing data
            self.councils = self.council_discovery.load_councils_data()
            self.analyses = self.website_analyzer.load_analyses()
            
            if not self.councils:
                raise ValueError("No council data found. Run council discovery first.")
            
            await self._step_extract_data(max_councils)
            
            summary = {
                'step': 'extraction_only',
                'start_time': start_time.isoformat(),
                'duration': (datetime.now() - start_time).total_seconds(),
                'councils_processed': len(self.extraction_results),
                'successful_extractions': len([r for r in self.extraction_results if r.success]),
                'total_licences_found': sum(r.licences_found for r in self.extraction_results)
            }
            
            logger.info("Data extraction completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            raise
    
    async def generate_reports_only(self) -> Dict[str, Any]:
        """Generate reports from existing processed data"""
        logger.info("Generating reports from existing data")
        start_time = datetime.now()
        
        try:
            # Load latest processed data
            data_dir = Path(self.settings.data_dir) / "licences"
            processed_files = list(data_dir.glob("processed_licences_*.json"))
            
            if not processed_files:
                raise ValueError("No processed data found. Run data processing first.")
            
            latest_file = max(processed_files, key=lambda f: f.stat().st_mtime)
            
            self.processed_licences, _ = self.data_processor.load_processed_data(str(latest_file))
            
            await self._step_generate_reports()
            
            summary = {
                'step': 'reports_only',
                'start_time': start_time.isoformat(),
                'duration': (datetime.now() - start_time).total_seconds(),
                'licences_processed': len(self.processed_licences),
                'reports_generated': 2  # Weekly and full dataset reports
            }
            
            logger.info("Report generation completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            raise
    
    async def _step_discover_councils(self):
        """Step 1: Discover UK councils and their websites"""
        logger.info("Step 1: Discovering UK councils")
        
        # Discover councils
        self.councils = self.council_discovery.discover_councils()
        logger.info(f"Discovered {len(self.councils)} councils")
        
        # Find licence registers using AI
        self.councils = await self.council_discovery.discover_licence_registers()
        
        successful_councils = len([c for c in self.councils if c.licence_register_url])
        logger.info(f"Found licence registers for {successful_councils} councils")
        
        # Save progress
        self.council_discovery.save_councils_data()
    
    async def _step_analyze_websites(self):
        """Step 2: Analyze council websites"""
        logger.info("Step 2: Analyzing council websites")
        
        # Filter councils with licence register URLs
        councils_with_registers = [c for c in self.councils if c.licence_register_url]
        
        if not councils_with_registers:
            logger.warning("No councils with licence registers found")
            return
        
        # Analyze websites
        self.analyses = await self.website_analyzer.analyze_council_websites(councils_with_registers)
        
        successful_analyses = len([a for a in self.analyses if a.licence_register_found])
        logger.info(f"Successfully analyzed {successful_analyses} websites")
    
    async def _step_extract_data(self, max_councils: Optional[int] = None):
        """Step 3: Extract premises licence data"""
        logger.info("Step 3: Extracting premises licence data")
        
        # Filter councils for extraction
        extraction_councils = [
            c for c in self.councils 
            if c.licence_register_url and c.scrape_successful
        ]
        
        if max_councils:
            extraction_councils = extraction_councils[:max_councils]
            logger.info(f"Limited to {max_councils} councils for extraction")
        
        if not extraction_councils:
            logger.warning("No councils available for extraction")
            return
        
        logger.info(f"Extracting data from {len(extraction_councils)} councils")
        
        # Run extraction
        self.extraction_results = await self.data_extractor.extract_all_licences(
            extraction_councils, self.analyses
        )
        
        successful_extractions = len([r for r in self.extraction_results if r.success])
        total_licences = sum(r.licences_found for r in self.extraction_results)
        
        logger.info(f"Extraction completed: {successful_extractions}/{len(self.extraction_results)} successful, {total_licences} licences found")
    
    async def _step_process_data(self):
        """Step 4: Process and clean extracted data"""
        logger.info("Step 4: Processing and cleaning data")
        
        if not self.extraction_results:
            logger.warning("No extraction results to process")
            return
        
        # Process data
        original_count = sum(len(r.licences_extracted) for r in self.extraction_results if r.licences_extracted)
        
        self.processed_licences = self.data_processor.process_extraction_results(self.extraction_results)
        
        logger.info(f"Data processing completed: {original_count} -> {len(self.processed_licences)} licences")
        
        # Generate and save processing summary
        summary = self.data_processor.generate_processing_summary(original_count, self.processed_licences)
        
        # Save processed data
        filename = self.data_processor.save_processed_data(self.processed_licences, summary)
        logger.info(f"Processed data saved to: {filename}")
    
    async def _step_generate_reports(self):
        """Step 5: Generate Excel reports"""
        logger.info("Step 5: Generating reports")
        
        if not self.processed_licences:
            logger.warning("No processed licences available for reporting")
            return
        
        # Generate weekly report
        weekly_report = self.report_generator.generate_weekly_report(self.processed_licences)
        logger.info(f"Weekly report generated: {weekly_report}")
        
        # Generate full dataset report
        full_report = self.report_generator.generate_full_dataset_report(self.processed_licences)
        logger.info(f"Full dataset report generated: {full_report}")
    
    def _generate_run_summary(self, start_time: datetime, incremental: bool = False) -> Dict[str, Any]:
        """Generate summary of the scraping run"""
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            'run_type': 'incremental' if incremental else 'full',
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': duration,
            'duration_minutes': duration / 60,
            'councils_discovered': len(self.councils),
            'councils_with_registers': len([c for c in self.councils if c.licence_register_url]),
            'successful_analyses': len([a for a in self.analyses if a.licence_register_found]),
            'extraction_results': len(self.extraction_results),
            'successful_extractions': len([r for r in self.extraction_results if r.success]),
            'total_licences_extracted': sum(r.licences_found for r in self.extraction_results),
            'final_processed_licences': len(self.processed_licences),
            'reports_generated': 2 if self.processed_licences else 0
        }
        
        # Add performance metrics
        if self.extraction_results:
            extraction_durations = [r.scraping_duration for r in self.extraction_results if r.scraping_duration]
            if extraction_durations:
                summary['avg_extraction_time'] = sum(extraction_durations) / len(extraction_durations)
                summary['total_extraction_time'] = sum(extraction_durations)
        
        # Add data quality metrics
        if self.processed_licences:
            complete_addresses = len([l for l in self.processed_licences if l.postcode])
            summary['data_quality'] = {
                'licences_with_postcodes': complete_addresses,
                'postcode_completion_rate': complete_addresses / len(self.processed_licences),
                'avg_activities_per_licence': sum(len(l.licensable_activities) for l in self.processed_licences) / len(self.processed_licences),
                'licences_with_conditions': len([l for l in self.processed_licences if l.conditions])
            }
        
        return summary
    
    def save_run_summary(self, summary: Dict[str, Any], filename: Optional[str] = None):
        """Save run summary to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.settings.data_dir}/summaries/run_summary_{timestamp}.json"
        
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str, ensure_ascii=False)
        
        logger.info(f"Run summary saved to: {filename}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check of the scraper system"""
        logger.info("Performing system health check")
        
        health = {
            'timestamp': datetime.now().isoformat(),
            'system_status': 'healthy',
            'components': {},
            'data_status': {},
            'recommendations': []
        }
        
        # Check OpenAI API
        try:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(model_name=self.settings.openai_model, api_key=self.settings.openai_api_key)
            response = await asyncio.to_thread(llm.invoke, [{"role": "user", "content": "Test"}])
            health['components']['openai_api'] = 'accessible'
        except Exception as e:
            health['components']['openai_api'] = f'error: {e}'
            health['system_status'] = 'degraded'
        
        # Check data directories
        data_paths = [
            self.settings.data_dir,
            self.settings.reports_dir,
            f"{self.settings.data_dir}/councils",
            f"{self.settings.data_dir}/licences"
        ]
        
        for path in data_paths:
            if Path(path).exists():
                health['components'][f'directory_{Path(path).name}'] = 'exists'
            else:
                health['components'][f'directory_{Path(path).name}'] = 'missing'
                health['recommendations'].append(f"Create missing directory: {path}")
        
        # Check existing data
        councils_file = f"{self.settings.data_dir}/councils/discovered_councils.json"
        if Path(councils_file).exists():
            with open(councils_file, 'r') as f:
                councils_data = json.load(f)
            health['data_status']['councils_discovered'] = len(councils_data)
            
            recent_councils = [
                c for c in councils_data 
                if c.get('last_scraped') and 
                datetime.fromisoformat(c['last_scraped']) > datetime.now() - timedelta(days=7)
            ]
            health['data_status']['recently_scraped_councils'] = len(recent_councils)
        else:
            health['data_status']['councils_discovered'] = 0
            health['recommendations'].append("Run council discovery to initialize system")
        
        # Check processed data
        data_dir = Path(self.settings.data_dir) / "licences"
        if data_dir.exists():
            processed_files = list(data_dir.glob("processed_licences_*.json"))
            health['data_status']['processed_data_files'] = len(processed_files)
            
            if processed_files:
                latest_file = max(processed_files, key=lambda f: f.stat().st_mtime)
                health['data_status']['latest_data_age_hours'] = (
                    datetime.now().timestamp() - latest_file.stat().st_mtime
                ) / 3600
                
                if health['data_status']['latest_data_age_hours'] > 168:  # 1 week
                    health['recommendations'].append("Data is over 1 week old, consider running incremental update")
        else:
            health['data_status']['processed_data_files'] = 0
            health['recommendations'].append("No processed data found, run full scrape")
        
        # Check reports
        reports_dir = Path(self.settings.reports_dir)
        if reports_dir.exists():
            report_files = list(reports_dir.glob("*.xlsx"))
            health['data_status']['report_files'] = len(report_files)
        else:
            health['data_status']['report_files'] = 0
        
        # Overall health assessment
        if len(health['recommendations']) > 0:
            if any('error' in str(status) for status in health['components'].values()):
                health['system_status'] = 'unhealthy'
            else:
                health['system_status'] = 'needs_attention'
        
        logger.info(f"Health check completed: {health['system_status']}")
        return health


# CLI Commands
async def run_full_scrape(max_councils: Optional[int] = None):
    """CLI command to run full scrape"""
    scraper = PremisesLicenceScraper()
    summary = await scraper.run_full_scrape(max_councils)
    scraper.save_run_summary(summary)
    return summary

async def run_incremental_update(days_back: int = 7):
    """CLI command to run incremental update"""
    scraper = PremisesLicenceScraper()
    summary = await scraper.run_incremental_update(days_back)
    scraper.save_run_summary(summary)
    return summary

async def run_discovery_only():
    """CLI command to run council discovery only"""
    scraper = PremisesLicenceScraper()
    summary = await scraper.run_council_discovery_only()
    scraper.save_run_summary(summary)
    return summary

async def run_extraction_only(max_councils: Optional[int] = None):
    """CLI command to run data extraction only"""
    scraper = PremisesLicenceScraper()
    summary = await scraper.run_data_extraction_only(max_councils)
    scraper.save_run_summary(summary)
    return summary

async def generate_reports():
    """CLI command to generate reports only"""
    scraper = PremisesLicenceScraper()
    summary = await scraper.generate_reports_only()
    scraper.save_run_summary(summary)
    return summary

async def health_check():
    """CLI command to perform health check"""
    scraper = PremisesLicenceScraper()
    health = await scraper.health_check()
    print(json.dumps(health, indent=2, default=str))
    return health


# Main CLI entry point
async def main():
    """Main CLI entry point"""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='UK Premises Licence Scraper')
    parser.add_argument('command', choices=[
        'full-scrape', 'incremental', 'discovery', 'extraction', 'reports', 'health-check'
    ], help='Command to run')
    parser.add_argument('--max-councils', type=int, help='Maximum number of councils to process')
    parser.add_argument('--days-back', type=int, default=7, help='Days back for incremental update')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{get_settings().data_dir}/scraper.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    try:
        if args.command == 'full-scrape':
            summary = await run_full_scrape(args.max_councils)
        elif args.command == 'incremental':
            summary = await run_incremental_update(args.days_back)
        elif args.command == 'discovery':
            summary = await run_discovery_only()
        elif args.command == 'extraction':
            summary = await run_extraction_only(args.max_councils)
        elif args.command == 'reports':
            summary = await generate_reports()
        elif args.command == 'health-check':
            summary = await health_check()
        
        print(f"\nCommand completed successfully!")
        print(f"Duration: {summary.get('duration_minutes', 0):.1f} minutes")
        
        if 'final_processed_licences' in summary:
            print(f"Final licences: {summary['final_processed_licences']}")
        
        if summary.get('recommendations'):
            print("\nRecommendations:")
            for rec in summary['recommendations']:
                print(f"- {rec}")
                
    except Exception as e:
        logger.error(f"Command failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
