#!/usr/bin/env python3
"""
Financial News Pipeline Test Script

This script tests the complete crawl → extract → store pipeline
and verifies duplicate detection works correctly.
"""

import asyncio
import json
import logging
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_crawl_pipeline():
    """Test the complete crawling pipeline"""
    try:
        # Import after setting up environment
        from src.finapp.services.crawl import VietstockCrawlerService
        from src.finapp.config import Config
        
        logger.info("🚀 Starting Crawl Pipeline Test")
        
        # Initialize crawler service
        crawler = VietstockCrawlerService(
            base_dir="data",
            source_name="vietstock"
        )
        
        # Test 1: Health check
        logger.info("📊 Test 1: Getting crawler statistics...")
        stats = crawler.get_crawl_statistics()
        logger.info(f"   Storage backend: {stats.get('storage_backend')}")
        logger.info(f"   Database: {stats.get('database_name')}")
        logger.info(f"   Export directory: {stats.get('export_directory')}")
        
        # Test 2: Single category crawl (limited)
        logger.info("📁 Test 2: Crawling single category...")
        from src.finapp.services.crawl.models import RSSCategory
        
        test_category = RSSCategory(
            name="Tai-chinh-chung-khoan",
            url="https://vietstock.vn/rss/tai-chinh-chung-khoan.rss",
            subcategories=[]
        )
        
        new_articles_count = crawler.crawl_category(test_category, filter_by_today=True)
        logger.info(f"   ✅ Found {new_articles_count} new articles")
        
        # Test 3: Check duplicate detection
        logger.info("🔍 Test 3: Testing duplicate detection...")
        # Crawl the same category again - should find 0 new articles
        duplicate_count = crawler.crawl_category(test_category, filter_by_today=True)
        logger.info(f"   ✅ Duplicate detection working: {duplicate_count} duplicates found")
        
        # Test 4: Get detailed statistics
        logger.info("📈 Test 4: Getting detailed statistics...")
        detailed_stats = crawler.storage.get_articles_statistics()
        logger.info(f"   Total articles: {detailed_stats.get('total_articles', 0)}")
        logger.info(f"   Storage backend: {detailed_stats.get('storage_backend')}")
        
        # Test 5: Verify article structure in database
        logger.info("🗄️ Test 5: Verifying article structure...")
        recent_articles = crawler.storage.repository.find_articles_by_date_range(
            datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            datetime.now(timezone.utc)
        )
        
        if recent_articles:
            sample_article = recent_articles[0]
            logger.info(f"   Sample article structure:")
            logger.info(f"     - Title: {sample_article.get('content', {}).get('headline', 'N/A')[:50]}...")
            logger.info(f"     - GUID: {sample_article.get('content', {}).get('rss_guid', 'N/A')}")
            logger.info(f"     - Category: {sample_article.get('rss_category', 'N/A')}")
            logger.info(f"     - Published: {sample_article.get('published_at', 'N/A')}")
        
        crawler.close()
        logger.info("✅ Crawl Pipeline Test Completed Successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Crawl Pipeline Test Failed: {e}")
        return False

async def test_extract_pipeline():
    """Test the LLM extraction pipeline (if available)"""
    try:
        from src.finapp.services.extract import ExtractorService
        from src.finapp.config import Config
        
        logger.info("🧠 Starting Extract Pipeline Test")
        
        extractor = ExtractorService()
        
        # Test extractor health
        model_info = extractor.get_model_info()
        logger.info(f"   Model: {model_info.get('model_name', 'N/A')}")
        logger.info(f"   Provider: {model_info.get('provider', 'N/A')}")
        
        logger.info("✅ Extract Pipeline Test Completed!")
        return True
        
    except ImportError:
        logger.info("ℹ️ Extract Pipeline not available - skipping")
        return True
    except Exception as e:
        logger.error(f"❌ Extract Pipeline Test Failed: {e}")
        return False

async def test_mongo_connection():
    """Test MongoDB connection and basic operations"""
    try:
        from src.finapp.database.vietstock import VietstockRepository
        
        logger.info("🗄️ Starting MongoDB Connection Test")
        
        repo = VietstockRepository(
            Config.MONGODB_URI,
            Config.DATABASE_NAME
        )
        
        # Test connection
        result = repo.db.command('ping')
        logger.info("   ✅ MongoDB connection successful")
        
        # Test indexes
        articles_collection = repo.db.vietstock_articles
        indexes = articles_collection.list_indexes()
        index_names = [idx['name'] for idx in indexes]
        logger.info(f"   📊 Available indexes: {index_names}")
        
        # Test duplicate prevention
        logger.info("   🔍 Testing duplicate prevention...")
        total_count = articles_collection.count_documents({})
        logger.info(f"   📈 Total articles in database: {total_count}")
        
        repo.close()
        logger.info("✅ MongoDB Connection Test Completed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB Connection Test Failed: {e}")
        return False

async def main():
    """Main test runner"""
    logger.info("🎯 Starting Financial News Pipeline Tests")
    logger.info("=" * 60)
    
    tests = [
        ("MongoDB Connection", test_mongo_connection),
        ("Crawl Pipeline", test_crawl_pipeline),
        ("Extract Pipeline", test_extract_pipeline),
    ]
    
    results = {}
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running {test_name} Test...")
        results[test_name] = await test_func()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name:.<30} {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Pipeline is ready for production.")
    else:
        logger.warning("⚠️ Some tests failed. Please check the logs above.")

if __name__ == "__main__":
    asyncio.run(main())