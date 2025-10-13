"""
Scheduler service for automated crawling
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from typing import Optional
import atexit

from .crawler import VietstockCrawlerService

logger = logging.getLogger(__name__)


class CrawlerScheduler:
    """Scheduler for automated RSS crawling"""
    
    def __init__(self, crawler_service: VietstockCrawlerService, interval_minutes: int = 5):
        self.crawler_service = crawler_service
        self.interval_minutes = interval_minutes
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        
        # Register cleanup on exit
        atexit.register(self.shutdown)
    
    def start(self, run_immediately: bool = True):
        """Start the scheduler"""
        if self.is_running:
            logger.warning("⚠️ Scheduler is already running")
            return
        
        try:
            # Schedule crawl job
            self.scheduler.add_job(
                func=self._crawl_job,
                trigger=IntervalTrigger(minutes=self.interval_minutes),
                id='vietstock_crawler',
                name='Vietstock RSS Crawler',
                max_instances=1,
                coalesce=True,
                replace_existing=True
            )
            
            # Start scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info(f"⏰ Scheduler started - crawling every {self.interval_minutes} minutes")
            
            # Run initial crawl if requested
            if run_immediately:
                logger.info("🚀 Running initial crawl")
                self._crawl_job()
                
        except Exception as e:
            logger.error(f"❌ Failed to start scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        if not self.is_running:
            logger.warning("⚠️ Scheduler is not running")
            return
        
        try:
            # Remove all jobs before shutdown
            try:
                self.scheduler.remove_all_jobs()
            except:
                pass  # Ignore errors when removing jobs
            
            self.scheduler.shutdown(wait=False)
            self.is_running = False
            logger.info("🛑 Scheduler stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping scheduler: {e}")
    
    def shutdown(self):
        """Cleanup method called on exit"""
        if self.is_running:
            self.stop()
    
    def trigger_manual_crawl(self):
        """Trigger a manual crawl job"""
        if not self.is_running:
            logger.warning("⚠️ Scheduler is not running. Start the scheduler first.")
            return False
        
        try:
            logger.info("🔄 Triggering manual crawl")
            self.scheduler.add_job(
                func=self._crawl_job,
                trigger='date',
                id='manual_crawl',
                name='Manual Vietstock Crawl'
            )
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to trigger manual crawl: {e}")
            return False
    
    def get_next_run_time(self) -> Optional[str]:
        """Get next scheduled run time"""
        if not self.is_running:
            return None
        
        try:
            job = self.scheduler.get_job('vietstock_crawler')
            if job:
                return job.next_run_time.isoformat() if job.next_run_time else None
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting next run time: {e}")
            return None
    
    def get_status(self) -> dict:
        """Get scheduler status"""
        return {
            'is_running': self.is_running,
            'interval_minutes': self.interval_minutes,
            'next_run_time': self.get_next_run_time(),
            'jobs_count': len(self.scheduler.get_jobs()) if self.is_running else 0
        }
    
    def _crawl_job(self):
        """Internal crawl job method"""
        try:
            logger.info("🚀 Starting scheduled crawl session")
            session = self.crawler_service.crawl_all_categories()
            
            if session.total_articles > 0:
                logger.info(f"✅ Scheduled crawl completed. New articles: {session.total_articles}")
            else:
                logger.info("ℹ️ Scheduled crawl completed. No new articles found")
                
        except Exception as e:
            logger.error(f"❌ Scheduled crawl failed: {e}")
    
    def update_interval(self, new_interval_minutes: int):
        """Update the crawl interval"""
        if new_interval_minutes < 1 or new_interval_minutes > 1440:
            raise ValueError("Interval must be between 1 and 1440 minutes")
        
        self.interval_minutes = new_interval_minutes
        
        if self.is_running:
            # Reschedule the job with new interval
            self.scheduler.reschedule_job(
                'vietstock_crawler',
                trigger=IntervalTrigger(minutes=self.interval_minutes)
            )
            logger.info(f"⏰ Updated crawl interval to {self.interval_minutes} minutes")