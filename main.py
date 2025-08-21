"""
UK Premises Licence Scraper - Main Entry Point

This application scrapes UK council websites to track newly granted premises licences
and future licence applications, providing business intelligence for commercial advantage.

Usage:
    uv run python main.py <command> [options]

Commands:
    full-scrape     - Run complete scraping workflow
    incremental     - Update with recent data only  
    discovery       - Discover councils and analyse websites only
    extraction      - Extract data from known councils only
    reports         - Generate reports from existing data
    health-check    - Check system health and data status

Examples:
    uv run python main.py full-scrape --max-councils 10
    uv run python main.py incremental --days-back 7
    uv run python main.py health-check
"""

import asyncio
import sys


def main():
    """Main entry point - delegates to orchestrator"""
    print("🏛️  UK Premises Licence Scraper")
    print("   AI-powered council website monitoring for business intelligence")
    print("")
    
    if len(sys.argv) < 2:
        print("Usage: uv run python main.py <command> [options]")
        print("")
        print("Available commands:")
        print("  full-scrape     Run complete scraping workflow")
        print("  incremental     Update with recent data only")
        print("  discovery       Discover councils and analyse websites only")
        print("  extraction      Extract data from known councils only") 
        print("  reports         Generate reports from existing data")
        print("  health-check    Check system health and data status")
        print("")
        print("Examples:")
        print("  uv run python main.py full-scrape --max-councils 10")
        print("  uv run python main.py incremental --days-back 7")
        print("  uv run python main.py health-check")
        print("")
        print("For detailed help: uv run python main.py <command> --help")
        return
    
    # Run the orchestrator
    from scraper.orchestrator import main as orchestrator_main
    asyncio.run(orchestrator_main())


if __name__ == "__main__":
    main()
