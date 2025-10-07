# Financial News Analysis System - API Backend

This system provides a FastAPI-based backend for financial news analysis that integrates with Windmill workflows and MongoDB.

## 🏗️ Refactored Architecture

The system has been refactored to follow a clean API-first approach:

```
┌─────────────────────────────────────────────────────────┐
│                   FASTAPI BACKEND                       │
│  ┌─────────────────┐  ┌─────────────────┐              │
│  │   API Routes    │  │  Error Handling │              │
│  │                 │  │  & Health Check │              │
│  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────┐
│                   SERVICE LAYER                         │
│  ┌─────────────────┐  ┌─────────────────┐              │
│  │ DatabaseService │  │ WindmillService │              │
│  │   (MongoDB)     │  │  (Workflows)    │              │
│  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────┐
│                 EXTERNAL SERVICES                       │
│  ┌─────────────────┐  ┌─────────────────┐              │
│  │    MongoDB      │  │    Windmill     │              │
│  │   Database      │  │   Workflows     │              │
│  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### 1. Installation & Setup

```bash
# Clone and navigate to the project
cd /root/projects/finapp

# Install dependencies
pip install -r requirements.txt

# Setup environment (copy and modify)
cp .env.example .env
# Edit .env with your MongoDB and Windmill configuration

# Start the API server
./start_api.sh
# OR
python src/api_backend.py
```

### 2. API Documentation

Once running, access the interactive API documentation:
- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc
- **Health Check**: http://localhost:8001/health

### 3. Test the API

```bash
# Run the test suite
python test_api.py
```

## 📊 API Endpoints

### Core Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | System health check |
| `/` | GET | API information |

### Windmill Integration

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/windmill/trigger` | POST | Trigger any Windmill workflow |
| `/windmill/trigger/news-crawling` | POST | Start news crawling |
| `/windmill/trigger/stock-analysis` | POST | Start stock analysis |
| `/windmill/trigger/sector-analysis` | POST | Start sector analysis |
| `/windmill/trigger/market-overview` | POST | Start market overview |
| `/windmill/llm-stream` | POST | Stream LLM requests via Windmill |

### Database Operations

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/database/query` | POST | Query MongoDB collections |
| `/database/insert` | POST | Insert documents |
| `/database/collections` | GET | List all collections |
| `/database/stats/{collection}` | GET | Collection statistics |

### Convenience Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/news/articles` | GET | Get news articles |
| `/reports/stocks` | GET | Get stock reports |
| `/reports/sectors` | GET | Get sector reports |
| `/reports/market` | GET | Get market reports |

## 🏗️ Core Components

### 1. API Backend (`src/api_backend.py`)

**Purpose**: FastAPI application providing all API endpoints.

**Key Features**:
- `BaseDocument`: Abstract base for all MongoDB documents
- `RawDocument`: Stores raw HTML/PDF content after crawling
- `NewsArticle`: Processed news with structured data
- `StockReport`: Individual stock analysis reports
- `SectorReport`: Sector-level analysis reports  
- `MarketReport`: Market overview reports

**Design Patterns**:
- **Data Classes**: Clean, immutable data structures
- **Enums**: Type-safe constants (ProcessingStatus, EventType, etc.)
- **Inheritance**: Common BaseDocument for all entities

```python
# Example: Clean data model with validation
class RawDocument(BaseDocument):
    def mark_processing(self):
        self.processing_status = ProcessingStatus.PROCESSING
    
    def mark_failed(self, error: str):
        self.processing_status = ProcessingStatus.FAILED
        self.error_message = error
```

### 2. Interface Layer (`interfaces.py`)

**Purpose**: Define contracts for all services using Abstract Base Classes.

**Key Interfaces**:
- `DataRepository`: Database operations contract
- `NewsExtractor`: News extraction contract
- `NewsAnalyzer`: Analysis generation contract  
- `NewsCrawler`: Content crawling contract
- `WorkflowOrchestrator`: Workflow coordination contract

**Design Patterns**:
- **Interface Segregation**: Small, focused interfaces
- **Dependency Inversion**: Code depends on abstractions, not concretions

```python
class NewsExtractor(ABC):
    @abstractmethod
    def extract(self, raw_document: RawDocument) -> NewsArticle:
        """Extract structured data from raw document"""
        pass
        
    @abstractmethod
    def validate_extraction(self, article: NewsArticle) -> bool:
        """Validate extracted article data"""
        pass
```

### 3. Service Layer (`services.py`)

**Purpose**: Concrete implementations of interfaces with business logic.

**Key Services**:
- `MongoDataRepository`: MongoDB implementation
- `LLMNewsExtractor`: LLM-based extraction service
- `FinancialNewsAnalyzer`: Analysis generation service
- `MainWorkflowOrchestrator`: Workflow coordination

**Design Patterns**:
- **Dependency Injection**: Services receive dependencies via constructor
- **Single Responsibility**: Each service has one clear purpose
- **Error Handling**: Consistent error handling and logging

```python
class LLMNewsExtractor(NewsExtractor):
    def __init__(self, llm_client, repository: DataRepository):
        self.llm_client = llm_client
        self.repository = repository
        self.logger = logging.getLogger(__name__)
    
    def extract(self, raw_document: RawDocument) -> NewsArticle:
        # Implementation with proper error handling
        try:
            raw_document.mark_processing()
            # ... extraction logic
            return article
        except Exception as e:
            raw_document.mark_failed(str(e))
            raise
```

### 4. Application Layer (`application.py`)

**Purpose**: Coordinate all components and provide main entry points.

**Key Classes**:
- `ApplicationConfig`: Configuration container
- `ServiceFactory`: Creates service instances with dependencies
- `FinancialNewsApplication`: Main application coordinator

**Design Patterns**:
- **Factory Pattern**: ServiceFactory creates configured instances
- **Facade Pattern**: Application class provides simple interface
- **Configuration Pattern**: Centralized configuration management

```python
class ServiceFactory:
    def create_news_extractor(self, repository: DataRepository) -> NewsExtractor:
        # Factory method with dependency injection
        return LLMNewsExtractor(self._openai_client, repository)

class FinancialNewsApplication:
    def __init__(self, config: ApplicationConfig):
        self.factory = ServiceFactory(config)
        self._initialize_services()
    
    def run_crawling_workflow(self) -> Dict[str, Any]:
        # High-level workflow coordination
        return self.orchestrator.run_crawling_workflow()
```

## 🔄 Workflow Processing

### 1. News Crawling Workflow

```python
# Simplified workflow showing OOP interaction
def run_crawling_workflow(self):
    # 1. Crawler gets raw content
    raw_documents = self.crawler.crawl_rss_feeds(feeds)
    
    # 2. Repository stores raw content  
    for raw_doc in raw_documents:
        self.repository.save(raw_doc)
        
        # 3. Extractor processes content
        article = self.extractor.extract(raw_doc)
        
        # 4. Validation and storage
        if self.extractor.validate_extraction(article):
            self.repository.save(article)
```

### 2. Analysis Workflow

```python
def run_analysis_workflow(self, time_window):
    # 1. Query relevant news
    articles = self.repository.find_by_criteria(criteria, NewsArticle)
    
    # 2. Group by company
    company_articles = self._group_by_company(articles)
    
    # 3. Generate stock reports
    stock_reports = []
    for company_id, articles in company_articles.items():
        report = self.analyzer.analyze_stock(company_id, ticker, articles, time_window)
        stock_reports.append(report)
    
    # 4. Generate sector and market reports
    sector_report = self.analyzer.analyze_sector("Tech", stock_reports)
    market_report = self.analyzer.analyze_market([sector_report])
```

## 🎯 Key OOP Principles Applied

### 1. **Single Responsibility Principle**
- Each class has one reason to change
- `NewsExtractor` only handles extraction
- `DataRepository` only handles data persistence
- `NewsAnalyzer` only handles analysis generation

### 2. **Open/Closed Principle** 
- Open for extension, closed for modification
- New extractors can implement `NewsExtractor` interface
- New repositories can implement `DataRepository` interface
- No need to modify existing code

### 3. **Liskov Substitution Principle**
- Subclasses can replace parent classes
- Any `DataRepository` implementation works with services
- Any `NewsExtractor` implementation works with orchestrator

### 4. **Interface Segregation Principle**
- Small, focused interfaces
- `NewsExtractor` separate from `NewsAnalyzer`
- `NewsCrawler` separate from `DataRepository`

### 5. **Dependency Inversion Principle**
- High-level modules don't depend on low-level modules
- Services depend on interfaces, not concrete implementations
- Easy to swap implementations for testing

## 🧪 Testing & Extensibility

### Mock Implementations (`demo.py`)
The system includes mock implementations for testing:

```python
class MockLLMClient:
    """Mock LLM for testing without external API calls"""
    def create_completion(self, **kwargs):
        return mock_response

class MockDatabase:
    """In-memory database for testing"""
    def __init__(self):
        self.collections = {}
```

### Easy Extension Points

1. **Add New Data Sources**:
   ```python
   class TwitterCrawler(NewsCrawler):
       def crawl_tweets(self, hashtags): ...
   ```

2. **Add New Analysis Types**:
   ```python
   class TechnicalAnalyzer(NewsAnalyzer):
       def analyze_charts(self, stock_data): ...
   ```

3. **Add New Storage Backends**:
   ```python
   class PostgreSQLRepository(DataRepository):
       def save(self, document): ...
   ```

## 🚀 Usage Examples

### Basic Usage
```python
# Create application
config = ApplicationConfig()
app = FinancialNewsApplication(config)

# Run workflows
crawl_result = app.run_crawling_workflow()
analysis_result = app.run_analysis_workflow("morning")

# Check status
status = app.get_status()
```

### Advanced Usage with Custom Services
```python
# Create custom factory
factory = ServiceFactory(config)

# Create services with custom implementations
repository = factory.create_repository()
extractor = CustomNewsExtractor(llm_client, repository)
analyzer = CustomAnalyzer(llm_client, repository)

# Create custom orchestrator
orchestrator = CustomOrchestrator(crawler, extractor, analyzer, repository)
```

## 📂 File Structure

```
src/
├── models_fixed.py      # Core data models
├── interfaces.py        # Abstract interfaces  
├── services.py          # Service implementations
├── simple_crawler.py    # RSS crawler implementation
├── application.py       # Main application & factory
└── demo.py             # Usage examples & testing

requirements.txt         # Python dependencies
README.md               # This documentation
```

## 🎖️ Benefits of This Architecture

1. **Maintainable**: Clear separation of concerns
2. **Testable**: Easy to mock dependencies  
3. **Extensible**: Add new features without breaking existing code
4. **Readable**: Self-documenting code with clear interfaces
5. **Flexible**: Easy to swap implementations
6. **Scalable**: Services can be distributed or replaced independently

This OOP structure provides a solid foundation for the Financial News Analysis System while maintaining simplicity and clarity in the codebase.

---

## API Backend Documentation

### Overview

The API backend is responsible for handling all incoming requests, processing them through the service layer, and returning the appropriate responses. It is built using FastAPI and is designed to be efficient, scalable, and easy to maintain.

### Features

- **Windmill Integration**: Triggers workflows for each report type
- **Database Operations**: Direct MongoDB query/insert capabilities  
- **Health Monitoring**: Comprehensive service health checks
- **Error Handling**: Global exception handling with structured responses
- **LLM Routing**: All LLM requests routed through Windmill streams
- **CORS Support**: Cross-origin resource sharing enabled

### 2. Database Service (`DatabaseService` class)

**Purpose**: MongoDB operations with robust error handling.

**Key Methods**:
- `insert_document()`: Insert with metadata and upsert support
- `query_documents()`: Flexible querying with pagination and sorting
- `get_collection_stats()`: Collection statistics and health info
- `health_check()`: Database connectivity and status

### 3. Windmill Service (`WindmillService` class)

**Purpose**: Windmill workflow integration and management.

**Key Methods**:
- `trigger_workflow()`: Start any Windmill flow with correlation tracking
- `stream_llm_request()`: Route LLM requests through Windmill API
- `health_check()`: Windmill service availability

### 4. Data Models (`src/models.py`)

**Purpose**: Define core data structures with clean OOP principles.

**Key Classes**:
- `RawDocument`: Unprocessed source documents
- `NewsArticle`: Extracted and structured news data
- `StockReport`, `SectorReport`, `MarketReport`: Analysis outputs
- `Sentiment`, `CompanyMention`, `ExtractedEvent`: Component data

### 5. Interfaces (`src/interfaces.py`) 

**Purpose**: Abstract contracts for service implementations.

**Key Interfaces**:
- `DataRepository`: Data access operations
- `NewsExtractor`: Content extraction services  
- `NewsAnalyzer`: Financial analysis services
- `NewsCrawler`: Content acquisition services

## 🔧 Configuration

### Environment Variables

```bash
# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
DATABASE_NAME=financial_news

# Windmill Configuration  
WINDMILL_BASE_URL=http://localhost:8000
WINDMILL_TOKEN=your_token_here
WINDMILL_WORKSPACE=finops

# API Configuration
API_HOST=0.0.0.0
API_PORT=8001
API_RELOAD=true
```

### Windmill Workflow Paths

The system expects these Windmill flows to be available:

- `news/crawling_workflow`: News content acquisition
- `analysis/stock_analysis`: Individual stock analysis
- `analysis/sector_analysis`: Sector-wide analysis
- `analysis/market_overview`: Market-wide overview

## 🔍 Key Design Decisions

### 1. **Workflow Orchestration Removed**
- ❌ Old: Backend orchestrated complex multi-step workflows
- ✅ New: Backend only triggers and monitors Windmill flows
- **Benefit**: Clear separation of concerns, easier maintenance

### 2. **LLM Service Routing**  
- ❌ Old: Direct OpenAI API calls from backend
- ✅ New: All LLM requests routed through Windmill streams
- **Benefit**: Centralized LLM management, better monitoring

### 3. **Database Direct Access**
- ✅ New: Direct MongoDB query/insert endpoints 
- **Benefit**: Flexibility for frontend and debugging

### 4. **Health & Monitoring**
- ✅ New: Comprehensive health checks for all services
- **Benefit**: Better observability and debugging

### 5. **Error Handling**
- ✅ New: Structured error responses with correlation IDs
- **Benefit**: Better debugging and user experience

## 🧪 Testing & Development

### Running Tests

```bash
# Start the API server
python src/api_backend.py

# In another terminal, run tests  
python test_api.py
```

### Development Workflow

1. **Make Changes**: Edit `src/api_backend.py` or related files
2. **Restart Server**: The API auto-reloads in development mode
3. **Test Changes**: Use `/docs` for interactive testing
4. **Run Test Suite**: Verify with `test_api.py`

### Adding New Endpoints

1. **Add Route Function**: Define new FastAPI route
2. **Add Request/Response Models**: Use Pydantic for validation  
3. **Add Service Logic**: Implement in appropriate service class
4. **Add Tests**: Update `test_api.py` with new test cases

## 🚀 Deployment

### Production Checklist

- [ ] Set production environment variables in `.env`
- [ ] Configure MongoDB connection with authentication
- [ ] Set up Windmill workspace and flows
- [ ] Configure reverse proxy (nginx/Apache) if needed
- [ ] Set up monitoring and logging
- [ ] Configure SSL/TLS certificates

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ ./src/
COPY .env .

EXPOSE 8001
CMD ["python", "src/api_backend.py"]
```

## 🔗 Integration

### Frontend Integration

The API provides a clean REST interface for frontend applications:

```javascript
// Trigger news crawling
await fetch('/windmill/trigger/news-crawling', {method: 'POST'});

// Query articles  
const articles = await fetch('/news/articles?limit=10').then(r => r.json());

// Health check
const health = await fetch('/health').then(r => r.json());
```

### Windmill Integration

Windmill flows should expect these API calls from the backend and can make callbacks:

```python
# In Windmill flow
def process_news_analysis(correlation_id: str, payload: dict):
    # Process the analysis
    results = analyze_news(payload)
    
    # Store results via backend API
    requests.post('http://backend:8001/database/insert', {
        'collection': 'analysis_results',
        'document': results
    })
```

## 📈 Monitoring & Observability 

### Health Monitoring

The `/health` endpoint provides comprehensive service status:

```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z", 
  "uptime_seconds": 3600,
  "services": {
    "database": {
      "status": "healthy",
      "connection": "active",
      "collections": 5
    },
    "windmill": {
      "status": "healthy", 
      "url": "http://localhost:8000",
      "version": "1.0.0"
    }
  }
}
```

### Logging

All services provide structured logging:

```python
logger.info("✅ MongoDB connected successfully")
logger.error(f"❌ Windmill workflow trigger failed: {error}")
```

### Correlation IDs

All Windmill workflows receive correlation IDs for request tracing:

```json
{
  "workflow_id": "wf_123456", 
  "correlation_id": "corr_789012"
}
```

---

**Key Refactoring Benefits:**

1. ✅ **Clean Separation**: Backend focused on API, Windmill handles workflows
2. ✅ **Better Monitoring**: Health checks and structured error handling  
3. ✅ **Scalability**: Stateless API design with external workflow engine
4. ✅ **Maintainability**: Clear interfaces and single responsibility
5. ✅ **Flexibility**: Direct database access and configurable workflows
