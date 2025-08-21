"""
Test script to verify the fixes work
"""

import asyncio
import logging
from scraper.council_discovery import CouncilDiscovery
from scraper.website_analyzer import WebsiteAnalyzer
from scraper.data_extractor import DataExtractor

logging.basicConfig(level=logging.INFO)

async def test_fixes():
    print("🔧 Testing scraper fixes...")
    
    # Test URL cleaning
    print("\n1. Testing URL cleaning...")
    discovery = CouncilDiscovery()
    councils = discovery.load_councils_data()
    
    if councils:
        print(f"Loaded {len(councils)} councils")
        valid_urls = [c for c in councils if c.licence_register_url and str(c.licence_register_url).startswith('http')]
        print(f"Valid URLs: {len(valid_urls)}")
        
        # Show cleaned URLs
        for council in valid_urls[:3]:
            print(f"  {council.name}: {council.licence_register_url}")
    
    # Test website analysis with fixed session management
    print("\n2. Testing website analysis...")
    analyzer = WebsiteAnalyzer()
    test_councils = [c for c in councils if c.licence_register_url and str(c.licence_register_url).startswith('http')][:2]
    
    if test_councils:
        analyses = await analyzer.analyze_council_websites(test_councils)
        print(f"Analyzed {len(analyses)} websites successfully")
        
        for analysis in analyses:
            print(f"  {analysis.council_name}: {'✅' if analysis.licence_register_found else '❌'}")
    
    # Test small extraction
    print("\n3. Testing data extraction...")
    extractor = DataExtractor()
    
    if test_councils:
        results = await extractor.extract_all_licences(test_councils[:1], analyses[:1])
        print(f"Extraction results: {len(results)}")
        
        for result in results:
            print(f"  {result.council_name}: {'✅' if result.success else '❌'} ({result.licences_found} licences)")
            if result.error_message:
                print(f"    Error: {result.error_message}")
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    asyncio.run(test_fixes())
