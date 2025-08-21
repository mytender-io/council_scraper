"""
Fix and clean the existing council URLs
"""

import json
import re
from scraper.council_discovery import CouncilDiscovery

def clean_url(messy_url):
    """Extract clean URL from messy AI response"""
    if not messy_url:
        return None
        
    messy_url = str(messy_url)
    
    # Find URLs in the response using regex
    url_pattern = r'https?://[^\s\]\)\n]+'
    urls = re.findall(url_pattern, messy_url)
    
    if urls:
        # Take the first valid URL found
        url = urls[0].rstrip('.,;:!?')  # Remove trailing punctuation
        
        # Remove common AI explanatory text
        url = url.replace('If%20this%20URL%20does%20not%20lead%20to%20the%20desired%20information,%20please%20check%20the%20council\'s%20website%20directly%20for%20the%20most%20accurate%20and%20updated%20links.', '')
        url = url.replace('If this URL does not lead to the desired information, please check the council\'s website directly for the most accurate and updated links.', '')
        
        # Additional cleaning
        if url.startswith('http') and not any(invalid in url for invalid in ['example.com', 'placeholder']):
            return url
            
    return None

def fix_council_urls():
    """Fix the existing council data URLs"""
    print("🔧 Fixing council URLs...")
    
    # Load existing data
    discovery = CouncilDiscovery()
    councils = discovery.load_councils_data()
    
    fixed_count = 0
    
    for council in councils:
        if council.licence_register_url:
            original_url = str(council.licence_register_url)
            cleaned_url = clean_url(original_url)
            
            if cleaned_url and cleaned_url != original_url:
                print(f"Fixing {council.name}:")
                print(f"  Before: {original_url}")
                print(f"  After:  {cleaned_url}")
                council.licence_register_url = cleaned_url
                fixed_count += 1
    
    print(f"\n✅ Fixed {fixed_count} URLs")
    
    # Add some known working URLs
    known_urls = {
        'Westminster City Council': 'https://www.westminster.gov.uk/licensing',
        'Islington Council': 'https://www.islington.gov.uk/business/licensing', 
        'Hackney Council': 'https://www.hackney.gov.uk/licensing',
        'Manchester City Council': 'https://www.manchester.gov.uk/info/200084/licensing',
        'Leeds City Council': 'https://www.leeds.gov.uk/business/licensing',
        'Liverpool City Council': 'https://liverpool.gov.uk/business/licensing/',
        'Bristol City Council': 'https://www.bristol.gov.uk/licences-permits/premises-licences',
        'Sheffield City Council': 'https://www.sheffield.gov.uk/home/business/licensing',
        'Newcastle City Council': 'https://www.newcastle.gov.uk/services/environmental-health/licensing',
        'Edinburgh Council': 'https://www.edinburgh.gov.uk/business/licensing'
    }
    
    manual_fixes = 0
    for council in councils:
        if council.name in known_urls:
            new_url = known_urls[council.name]
            if str(council.licence_register_url) != new_url:
                print(f"Manual fix {council.name}: {new_url}")
                council.licence_register_url = new_url
                council.scrape_successful = True
                manual_fixes += 1
    
    print(f"✅ Applied {manual_fixes} manual fixes")
    
    # Save the fixed data
    discovery.councils = councils
    discovery.save_councils_data()
    
    print("\n🎯 Fixed council URLs saved!")
    
    # Show summary
    working_councils = [c for c in councils if c.licence_register_url and str(c.licence_register_url).startswith('http') and len(str(c.licence_register_url)) < 200]
    print(f"\n📊 Summary:")
    print(f"Total councils: {len(councils)}")  
    print(f"With URLs: {len([c for c in councils if c.licence_register_url])}")
    print(f"Clean URLs: {len(working_councils)}")
    
    return working_councils

if __name__ == "__main__":
    working_councils = fix_council_urls()
    
    # Show working URLs
    print(f"\n🎯 Working URLs ({len(working_councils)}):")
    for council in working_councils:
        print(f"  ✅ {council.name}: {council.licence_register_url}")
