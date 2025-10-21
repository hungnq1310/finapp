"""
Master JSON Service for Daily Financial Data Aggregation

This service manages a master JSON file for each day that aggregates all extraction results
with 3-level organization and full metadata traceability.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, date
from collections import defaultdict, Counter
import logging
import uuid

logger = logging.getLogger(__name__)


class MasterJSONService:
    """Service for managing daily master JSON files with organized 3-level data"""
    
    def __init__(self, base_dir: Optional[str] = None):
        # Load from config if not provided
        resolved_base_dir: str
        if base_dir is None:
            from ...config import Config
            config = Config()
            resolved_base_dir = config.MASTER_JSON_DIR
        else:
            resolved_base_dir = base_dir
        
        self.base_dir = Path(resolved_base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Directory for current month
        today = datetime.now()
        self.current_month_dir = self.base_dir / f"{today.year:04d}/{today.month:02d}"
        self.current_month_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"MasterJSONService initialized with base_dir: {self.base_dir}")
    
    def get_master_file_path(self, target_date: str) -> Path:
        """Get the path to master JSON file for a specific date"""
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        year_month_dir = self.base_dir / f"{date_obj.year:04d}/{date_obj.month:02d}"
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        return year_month_dir / f"master_{target_date}.json"
    
    def initialize_daily_master(self, target_date: str) -> Dict[str, Any]:
        """Initialize a new master JSON file for the day"""
        return {
            "metadata": {
                "date": target_date,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "version": "1.0",
                "total_articles": 0,
                "successful_extractions": 0,
                "failed_extractions": 0
            },
            "summary": {
                "sentiment_overview": {
                    "tích_cực": {"count": 0, "percentage": 0},
                    "tiêu_cực": {"count": 0, "percentage": 0},
                    "trung_lập": {"count": 0, "percentage": 0}
                },
                "market_impact": {
                    "market_moving_articles": 0,
                    "high_impact_articles": 0,
                    "total_market_impact_score": 0
                },
                "top_stocks": [],  # Top 10 most mentioned stocks
                "top_sectors": [],  # Top 10 most affected sectors
                "financial_metrics": {
                    "articles_with_numbers": 0,
                    "total_revenue_mentions": 0,
                    "total_profit_mentions": 0,
                    "total_percentage_mentions": 0
                }
            },
            "indexes": {
                "by_ticker": {},      # ticker -> [article_indices]
                "by_sector": {},      # sector -> [article_indices]  
                "by_sentiment": {},   # sentiment -> [article_indices]
                "by_market_impact": {}, # impact_type -> [article_indices]
                "by_time": []         # chronological list of all article indices
            },
            "articles": [],  # Array of all extracted articles with full data
            "lookup_table": {}  # article_guid -> article_index for fast lookup
        }
    
    def append_extraction_to_master(self, target_date: str, extraction_result: Dict[str, Any], article_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Append a new extraction result to the daily master JSON file
        
        Args:
            target_date: Date in YYYY-MM-DD format
            extraction_result: The extraction result from LLM
            article_metadata: Original article metadata (title, content, etc.)
            
        Returns:
            Updated master data summary
        """
        try:
            master_file = self.get_master_file_path(target_date)
            
            # Load existing master or create new
            if master_file.exists():
                with open(master_file, 'r', encoding='utf-8') as f:
                    master_data = json.load(f)
            else:
                master_data = self.initialize_daily_master(target_date)
            
            # Create article entry with full data and metadata
            article_index = len(master_data["articles"])
            article_guid = article_metadata.get("guid", f"unknown_{uuid.uuid4().hex[:8]}")
            
            # Prepare the complete article entry
            article_entry = {
                "index": article_index,
                "guid": article_guid,
                "extraction_timestamp": extraction_result.get("extraction_timestamp"),
                "extraction_model": extraction_result.get("extraction_model"),
                "extraction_confidence": extraction_result.get("extraction_confidence"),
                
                # Original metadata for traceability
                "source": {
                    "title": article_metadata.get("title", ""),
                    "category": article_metadata.get("category", ""),
                    "description": article_metadata.get("description_text", ""),
                    "url": article_metadata.get("link", ""),
                    "pub_date": article_metadata.get("pub_date", ""),
                    "author": article_metadata.get("author", "")
                },
                
                # Full content for reference
                "content": {
                    "full_text": article_metadata.get("main_content", ""),
                    "description_text": article_metadata.get("description_text", "")
                },
                
                # 3-level extraction results
                "level_1_sentiment": extraction_result.get("sentiment_analysis", {}),
                "level_2_stocks": extraction_result.get("stock_level", []),
                "level_3_sectors": extraction_result.get("sector_level", []),
                "level_4_market": extraction_result.get("market_level", {}),
                "financial_data": extraction_result.get("financial_data", {}),
                
                # Quick access fields for filtering
                "quick_access": {
                    "tickers": [stock.get("ticker", "") for stock in extraction_result.get("stock_level", [])],
                    "sectors": [sector.get("sector_name", "") for sector in extraction_result.get("sector_level", [])],
                    "overall_sentiment": extraction_result.get("sentiment_analysis", {}).get("overall_sentiment", "trung_lập"),
                    "sentiment_score": extraction_result.get("sentiment_analysis", {}).get("sentiment_score", 0),
                    "is_market_moving": extraction_result.get("market_level", {}).get("market_moving", False),
                    "has_financial_numbers": extraction_result.get("financial_data", {}).get("has_numbers", False),
                    "confidence_score": extraction_result.get("extraction_confidence", 0)
                }
            }
            
            # Append to articles array
            master_data["articles"].append(article_entry)
            
            # Update lookup table
            master_data["lookup_table"][article_guid] = article_index
            
            # Update indexes
            self._update_indexes(master_data, article_entry, article_index)
            
            # Update summary statistics
            self._update_summary(master_data, article_entry)
            
            # Update metadata
            master_data["metadata"]["last_updated"] = datetime.now().isoformat()
            master_data["metadata"]["total_articles"] += 1
            if extraction_result.get("extraction_confidence", 0) > 0.5:  # Consider successful if confidence > 0.5
                master_data["metadata"]["successful_extractions"] += 1
            else:
                master_data["metadata"]["failed_extractions"] += 1
            
            # Save master file
            with open(master_file, 'w', encoding='utf-8') as f:
                json.dump(master_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Added article {article_guid} to master file for {target_date}")
            
            return {
                "success": True,
                "article_index": article_index,
                "master_file": str(master_file),
                "total_articles": master_data["metadata"]["total_articles"],
                "summary": master_data["summary"]
            }
            
        except Exception as e:
            logger.error(f"Failed to append extraction to master file for {target_date}: {e}")
            return {"success": False, "error": str(e)}
    
    def _update_indexes(self, master_data: Dict, article_entry: Dict, article_index: int):
        """Update all search indexes for the new article"""
        
        # Index by tickers
        for ticker in article_entry["quick_access"]["tickers"]:
            if ticker not in master_data["indexes"]["by_ticker"]:
                master_data["indexes"]["by_ticker"][ticker] = []
            master_data["indexes"]["by_ticker"][ticker].append(article_index)
        
        # Index by sectors
        for sector in article_entry["quick_access"]["sectors"]:
            if sector not in master_data["indexes"]["by_sector"]:
                master_data["indexes"]["by_sector"][sector] = []
            master_data["indexes"]["by_sector"][sector].append(article_index)
        
        # Index by sentiment
        sentiment = article_entry["quick_access"]["overall_sentiment"]
        if sentiment not in master_data["indexes"]["by_sentiment"]:
            master_data["indexes"]["by_sentiment"][sentiment] = []
        master_data["indexes"]["by_sentiment"][sentiment].append(article_index)
        
        # Index by market impact
        if article_entry["quick_access"]["is_market_moving"]:
            if "market_moving" not in master_data["indexes"]["by_market_impact"]:
                master_data["indexes"]["by_market_impact"]["market_moving"] = []
            master_data["indexes"]["by_market_impact"]["market_moving"].append(article_index)
        
        # Add to chronological index
        master_data["indexes"]["by_time"].append(article_index)
    
    def _update_summary(self, master_data: Dict, article_entry: Dict):
        """Update summary statistics"""
        summary = master_data["summary"]
        
        # Update sentiment overview
        sentiment = article_entry["quick_access"]["overall_sentiment"]
        if sentiment in summary["sentiment_overview"]:
            summary["sentiment_overview"][sentiment]["count"] += 1
        
        # Update market impact
        if article_entry["quick_access"]["is_market_moving"]:
            summary["market_impact"]["market_moving_articles"] += 1
        
        # Update financial metrics
        if article_entry["quick_access"]["has_financial_numbers"]:
            summary["financial_metrics"]["articles_with_numbers"] += 1
        
        financial_data = article_entry.get("financial_data", {})
        if financial_data.get("revenues"):
            summary["financial_metrics"]["total_revenue_mentions"] += len(financial_data["revenues"])
        if financial_data.get("profits"):
            summary["financial_metrics"]["total_profit_mentions"] += len(financial_data["profits"])
        if financial_data.get("percentages"):
            summary["financial_metrics"]["total_percentage_mentions"] += len(financial_data["percentages"])
        
        # Update percentages
        total_articles = master_data["metadata"]["total_articles"] + 1
        for sentiment_key in summary["sentiment_overview"]:
            count = summary["sentiment_overview"][sentiment_key]["count"]
            summary["sentiment_overview"][sentiment_key]["percentage"] = round((count / total_articles) * 100, 2)
        
        # Update top stocks (keep top 10)
        all_tickers = []
        for ticker, indices in master_data["indexes"]["by_ticker"].items():
            all_tickers.append((ticker, len(indices)))
        
        all_tickers.sort(key=lambda x: x[1], reverse=True)
        summary["top_stocks"] = [{"ticker": ticker, "mention_count": count} for ticker, count in all_tickers[:10]]
        
        # Update top sectors (keep top 10)
        all_sectors = []
        for sector, indices in master_data["indexes"]["by_sector"].items():
            all_sectors.append((sector, len(indices)))
        
        all_sectors.sort(key=lambda x: x[1], reverse=True)
        summary["top_sectors"] = [{"sector": sector, "article_count": count} for sector, count in all_sectors[:10]]
    
    def query_master_data(self, target_date: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Query master JSON data with filters
        
        Args:
            target_date: Date in YYYY-MM-DD format
            query_params: Optional filters (tickers, sectors, sentiments, etc.)
            
        Returns:
            Filtered results with full data and metadata
        """
        try:
            master_file = self.get_master_file_path(target_date)
            if not master_file.exists():
                return {"success": False, "error": f"No master data found for {target_date}"}
            
            with open(master_file, 'r', encoding='utf-8') as f:
                master_data = json.load(f)
            
            if not query_params:
                # Return summary only
                return {
                    "success": True,
                    "date": target_date,
                    "metadata": master_data["metadata"],
                    "summary": master_data["summary"],
                    "total_articles": len(master_data["articles"])
                }
            
            # Filter articles based on query params
            filtered_indices = set(range(len(master_data["articles"])))  # Start with all indices
            
            # Filter by tickers
            if query_params.get("tickers"):
                ticker_indices = set()
                for ticker in query_params["tickers"]:
                    ticker_indices.update(master_data["indexes"]["by_ticker"].get(ticker, []))
                filtered_indices &= ticker_indices
            
            # Filter by sectors
            if query_params.get("sectors"):
                sector_indices = set()
                for sector in query_params["sectors"]:
                    sector_indices.update(master_data["indexes"]["by_sector"].get(sector, []))
                filtered_indices &= sector_indices
            
            # Filter by sentiments
            if query_params.get("sentiments"):
                sentiment_indices = set()
                for sentiment in query_params["sentiments"]:
                    sentiment_indices.update(master_data["indexes"]["by_sentiment"].get(sentiment, []))
                filtered_indices &= sentiment_indices
            
            # Filter by market moving
            if query_params.get("market_moving_only"):
                market_moving_indices = set(master_data["indexes"]["by_market_impact"].get("market_moving", []))
                filtered_indices &= market_moving_indices
            
            # Filter by confidence score
            if query_params.get("min_confidence"):
                high_confidence_indices = set()
                for i, article in enumerate(master_data["articles"]):
                    if article.get("extraction_confidence", 0) >= query_params["min_confidence"]:
                        high_confidence_indices.add(i)
                filtered_indices &= high_confidence_indices
            
            # Sort indices (chronological by default)
            sorted_indices = sorted(list(filtered_indices))
            
            # Limit results if specified
            if query_params.get("limit"):
                sorted_indices = sorted_indices[:query_params["limit"]]
            
            # Collect filtered articles
            filtered_articles = []
            for idx in sorted_indices:
                article = master_data["articles"][idx].copy()
                
                # Optionally include full content
                if not query_params.get("include_full_content", False):
                    article["content"]["full_text"] = article["content"]["full_text"][:500] + "..." if len(article["content"]["full_text"]) > 500 else article["content"]["full_text"]
                
                filtered_articles.append(article)
            
            # Generate query summary
            query_summary = {
                "total_articles_available": len(master_data["articles"]),
                "articles_filtered": len(filtered_indices),
                "articles_returned": len(filtered_articles),
                "filters_applied": query_params,
                "query_timestamp": datetime.now().isoformat()
            }
            
            return {
                "success": True,
                "date": target_date,
                "metadata": master_data["metadata"],
                "summary": master_data["summary"],
                "query_summary": query_summary,
                "articles": filtered_articles
            }
            
        except Exception as e:
            logger.error(f"Failed to query master data for {target_date}: {e}")
            return {"success": False, "error": str(e)}
    
    def get_stock_analysis(self, target_date: str, ticker: str, include_full_content: bool = False) -> Dict[str, Any]:
        """Get comprehensive analysis for a specific stock"""
        query_params = {
            "tickers": [ticker],
            "include_full_content": include_full_content
        }
        
        result = self.query_master_data(target_date, query_params)
        if not result["success"]:
            return result
        
        articles = result["articles"]
        
        # Generate stock-specific insights
        sentiments = [a["quick_access"]["overall_sentiment"] for a in articles]
        sentiment_counts = Counter(sentiments)
        
        confidence_scores = [a["quick_access"]["confidence_score"] for a in articles]
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        # Impact analysis
        impact_types = []
        for article in articles:
            stock_info = next((s for s in article["level_2_stocks"] if s.get("ticker") == ticker), {})
            if stock_info:
                impact_types.append(stock_info.get("impact_type", ""))
        
        impact_counts = Counter(impact_types)
        
        # Price impact predictions
        price_impacts = []
        for article in articles:
            stock_info = next((s for s in article["level_2_stocks"] if s.get("ticker") == ticker), {})
            if stock_info:
                price_impacts.append(stock_info.get("price_impact", ""))
        
        price_impact_counts = Counter(price_impacts)
        
        return {
            "success": True,
            "date": target_date,
            "ticker": ticker,
            "analysis": {
                "total_articles": len(articles),
                "sentiment_distribution": dict(sentiment_counts),
                "avg_confidence": round(avg_confidence, 3),
                "impact_type_distribution": dict(impact_counts),
                "price_impact_distribution": dict(price_impact_counts),
                "insights": [
                    f"Có {len(articles)} bài báo về {ticker} trong ngày {target_date}",
                    f"Tâm lý chủ đạo: {sentiments[0] if sentiments else 'N/A'}",
                    f"Độ tin cậy trung bình: {avg_confidence:.2f}",
                    f"Xu hướng giá: {price_impacts[0] if price_impacts else 'chưa xác định'}"
                ]
            },
            "articles": articles
        }
    
    def get_sector_analysis(self, target_date: str, sector: str, include_full_content: bool = False) -> Dict[str, Any]:
        """Get comprehensive analysis for a specific sector"""
        query_params = {
            "sectors": [sector],
            "include_full_content": include_full_content
        }
        
        result = self.query_master_data(target_date, query_params)
        if not result["success"]:
            return result
        
        articles = result["articles"]
        
        # Collect all affected companies
        all_companies = set()
        for article in articles:
            companies = article.get("level_3_sectors", [])
            for sector_info in companies:
                if sector_info.get("sector_name") == sector:
                    all_companies.update(sector_info.get("affected_companies", []))
        
        # Sentiment analysis
        sentiments = [a["quick_access"]["overall_sentiment"] for a in articles]
        sentiment_counts = Counter(sentiments)
        
        return {
            "success": True,
            "date": target_date,
            "sector": sector,
            "analysis": {
                "total_articles": len(articles),
                "sentiment_distribution": dict(sentiment_counts),
                "total_companies_affected": len(all_companies),
                "affected_companies": sorted(list(all_companies)),
                "insights": [
                    f"Ngành {sector} có {len(articles)} bài báo trong ngày {target_date}",
                    f"Tâm lý chủ đạo: {sentiments[0] if sentiments else 'N/A'}",
                    f"Ảnh hưởng đến {len(all_companies)} công ty"
                ]
            },
            "articles": articles
        }
    
    def get_available_dates(self) -> Dict[str, Any]:
        """Get list of all available dates with master data"""
        try:
            available_dates = []
            
            for year_dir in self.base_dir.iterdir():
                if year_dir.is_dir() and year_dir.name.isdigit():
                    for month_dir in year_dir.iterdir():
                        if month_dir.is_dir() and month_dir.name.isdigit():
                            for master_file in month_dir.glob("master_*.json"):
                                date_str = master_file.stem.replace("master_", "")
                                try:
                                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                    
                                    # Get basic stats without loading full file
                                    file_size = master_file.stat().st_size / (1024 * 1024)  # MB
                                    
                                    available_dates.append({
                                        "date": date_str,
                                        "formatted_date": date_obj.strftime("%d/%m/%Y"),
                                        "file_path": str(master_file),
                                        "file_size_mb": round(file_size, 2),
                                        "year": int(year_dir.name),
                                        "month": int(month_dir.name)
                                    })
                                except ValueError:
                                    continue
            
            # Sort by date (newest first)
            available_dates.sort(key=lambda x: x["date"], reverse=True)
            
            return {
                "success": True,
                "total_dates": len(available_dates),
                "dates": available_dates
            }
            
        except Exception as e:
            logger.error(f"Failed to get available dates: {e}")
            return {"success": False, "error": str(e)}
    
    def export_report_data(self, target_date: str, tickers: Optional[List[str]] = None, sectors: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Export data formatted for report writing
        
        Args:
            target_date: Date in YYYY-MM-DD format
            tickers: Optional list of tickers to focus on
            sectors: Optional list of sectors to focus on
            
        Returns:
            Report-ready data with sources and insights
        """
        try:
            query_params = {}
            if tickers:
                query_params["tickers"] = tickers
            if sectors:
                query_params["sectors"] = sectors
            
            query_params["include_full_content"] = True
            
            result = self.query_master_data(target_date, query_params)
            if not result["success"]:
                return result
            
            articles = result["articles"]
            
            # Organize data for report writing
            report_data = {
                "report_metadata": {
                    "date": target_date,
                    "generated_at": datetime.now().isoformat(),
                    "focus_areas": {
                        "tickers": tickers or [],
                        "sectors": sectors or []
                    },
                    "total_articles": len(articles)
                },
                "executive_summary": self._generate_executive_summary(articles),
                "stock_analysis": self._generate_stock_analysis_for_report(articles, tickers),
                "sector_analysis": self._generate_sector_analysis_for_report(articles, sectors),
                "market_overview": self._generate_market_overview_for_report(articles),
                "detailed_articles": articles,  # Full articles with sources
                "appendix": {
                    "all_tickers_mentioned": list(set([ticker for article in articles for ticker in article["quick_access"]["tickers"]])),
                    "all_sectors_mentioned": list(set([sector for article in articles for sector in article["quick_access"]["sectors"]])),
                    "sentiment_distribution": Counter([article["quick_access"]["overall_sentiment"] for article in articles])
                }
            }
            
            return {
                "success": True,
                "report_data": report_data
            }
            
        except Exception as e:
            logger.error(f"Failed to export report data for {target_date}: {e}")
            return {"success": False, "error": str(e)}
    
    def _generate_executive_summary(self, articles: List[Dict]) -> Dict[str, Any]:
        """Generate executive summary for report"""
        total = len(articles)
        sentiments = [a["quick_access"]["overall_sentiment"] for a in articles]
        sentiment_counts = Counter(sentiments)
        
        market_moving = sum(1 for a in articles if a["quick_access"]["is_market_moving"])
        with_numbers = sum(1 for a in articles if a["quick_access"]["has_financial_numbers"])
        
        return {
            "total_articles_analyzed": total,
            "market_sentiment": {
                "primary": sentiments[0] if sentiments else "trung_lập",
                "distribution": dict(sentiment_counts)
            },
            "key_metrics": {
                "market_moving_articles": market_moving,
                "articles_with_financial_data": with_numbers,
                "avg_confidence": sum([a["quick_access"]["confidence_score"] for a in articles]) / total if total > 0 else 0
            },
            "highlights": [
                f"Phân tích {total} bài báo tài chính trong ngày",
                f"Tâm lý thị trường chủ đạo: {sentiments[0] if sentiments else 'Trung lập'}",
                f"{market_moving} bài báo có khả năng làm thị trường biến động",
                f"{with_numbers} bài báo chứa số liệu tài chính cụ thể"
            ]
        }
    
    def _generate_stock_analysis_for_report(self, articles: List[Dict], focus_tickers: Optional[List[str]]) -> List[Dict]:
        """Generate stock-focused analysis for report"""
        stock_analysis = {}
        
        for article in articles:
            for stock in article["level_2_stocks"]:
                ticker = stock.get("ticker", "")
                if not ticker:
                    continue
                
                if focus_tickers and ticker not in focus_tickers:
                    continue
                
                if ticker not in stock_analysis:
                    stock_analysis[ticker] = {
                        "ticker": ticker,
                        "articles": [],
                        "sentiments": [],
                        "impacts": [],
                        "price_predictions": [],
                        "confidence_scores": []
                    }
                
                stock_analysis[ticker]["articles"].append({
                    "title": article["source"]["title"],
                    "sentiment": stock.get("sentiment", ""),
                    "impact_type": stock.get("impact_type", ""),
                    "price_impact": stock.get("price_impact", ""),
                    "confidence": stock.get("confidence", 0),
                    "source_title": article["source"]["title"],
                    "source_url": article["source"]["url"],
                    "publication_date": article["source"]["pub_date"]
                })
                
                stock_analysis[ticker]["sentiments"].append(stock.get("sentiment", ""))
                stock_analysis[ticker]["impacts"].append(stock.get("impact_type", ""))
                stock_analysis[ticker]["price_predictions"].append(stock.get("price_impact", ""))
                stock_analysis[ticker]["confidence_scores"].append(stock.get("confidence", 0))
        
        # Generate insights for each stock
        for ticker in stock_analysis:
            data = stock_analysis[ticker]
            sentiment_counts = Counter(data["sentiments"])
            impact_counts = Counter(data["impacts"])
            price_counts = Counter(data["price_predictions"])
            
            data["insights"] = {
                "total_mentions": len(data["articles"]),
                "primary_sentiment": data["sentiments"][0] if data["sentiments"] else "N/A",
                "sentiment_distribution": dict(sentiment_counts),
                "main_impact_type": data["impacts"][0] if data["impacts"] else "N/A",
                "price_trend": data["price_predictions"][0] if data["price_predictions"] else "N/A",
                "avg_confidence": sum(data["confidence_scores"]) / len(data["confidence_scores"]) if data["confidence_scores"] else 0
            }
        
        return list(stock_analysis.values())
    
    def _generate_sector_analysis_for_report(self, articles: List[Dict], focus_sectors: Optional[List[str]]) -> List[Dict]:
        """Generate sector-focused analysis for report"""
        sector_analysis = {}
        
        for article in articles:
            for sector in article["level_3_sectors"]:
                sector_name = sector.get("sector_name", "")
                if not sector_name:
                    continue
                
                if focus_sectors and sector_name not in focus_sectors:
                    continue
                
                if sector_name not in sector_analysis:
                    sector_analysis[sector_name] = {
                        "sector": sector_name,
                        "articles": [],
                        "sentiments": [],
                        "affected_companies": set(),
                        "impact_descriptions": []
                    }
                
                sector_analysis[sector_name]["articles"].append({
                    "title": article["source"]["title"],
                    "sentiment": sector.get("sentiment", ""),
                    "impact_description": sector.get("impact_description", ""),
                    "affected_companies": sector.get("affected_companies", []),
                    "source_title": article["source"]["title"],
                    "source_url": article["source"]["url"]
                })
                
                sector_analysis[sector_name]["sentiments"].append(sector.get("sentiment", ""))
                sector_analysis[sector_name]["affected_companies"].update(sector.get("affected_companies", []))
                sector_analysis[sector_name]["impact_descriptions"].append(sector.get("impact_description", ""))
        
        # Generate insights for each sector
        for sector_name in sector_analysis:
            data = sector_analysis[sector_name]
            sentiment_counts = Counter(data["sentiments"])
            
            data["insights"] = {
                "total_articles": len(data["articles"]),
                "primary_sentiment": data["sentiments"][0] if data["sentiments"] else "N/A",
                "sentiment_distribution": dict(sentiment_counts),
                "total_companies_affected": len(data["affected_companies"]),
                "key_companies": list(data["affected_companies"])[:5],  # Top 5
                "main_impacts": [desc for desc in data["impact_descriptions"] if desc][:3]  # Top 3 impacts
            }
            
            # Convert set to list for JSON serialization
            data["affected_companies"] = list(data["affected_companies"])
        
        return list(sector_analysis.values())
    
    def _generate_market_overview_for_report(self, articles: List[Dict]) -> Dict[str, Any]:
        """Generate market overview for report"""
        market_moving_articles = [a for a in articles if a["quick_access"]["is_market_moving"]]
        high_confidence_articles = [a for a in articles if a["quick_access"]["confidence_score"] >= 0.8]
        
        # Market impact analysis
        market_scopes = []
        exchanges = []
        
        for article in articles:
            market_level = article.get("level_4_market", {})
            if market_level.get("scope"):
                market_scopes.append(market_level["scope"])
            if market_level.get("exchange"):
                exchanges.append(market_level["exchange"])
        
        scope_counts = Counter(market_scopes)
        exchange_counts = Counter(exchanges)
        
        return {
            "market_moving_analysis": {
                "total_market_moving_articles": len(market_moving_articles),
                "percentage_of_total": round((len(market_moving_articles) / len(articles)) * 100, 2) if articles else 0,
                "high_confidence_insights": len(high_confidence_articles),
                "key_market_moving_articles": [
                    {
                        "title": a["source"]["title"],
                        "sentiment": a["quick_access"]["overall_sentiment"],
                        "confidence": a["quick_access"]["confidence_score"],
                        "source_url": a["source"]["url"]
                    }
                    for a in market_moving_articles[:5]  # Top 5
                ]
            },
            "market_scope_distribution": dict(scope_counts),
            "exchange_distribution": dict(exchange_counts),
            "overall_market_health": {
                "sentiment_health": "positive" if scope_counts.get("toàn thị trường", 0) > len(articles) * 0.3 else "neutral",
                "confidence_health": "high" if sum([a["quick_access"]["confidence_score"] for a in articles]) / len(articles) > 0.7 else "moderate",
                "data_quality": "excellent" if len([a for a in articles if a["quick_access"]["has_financial_numbers"]]) > len(articles) * 0.5 else "good"
            }
        }