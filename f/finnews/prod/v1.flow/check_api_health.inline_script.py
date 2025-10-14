import requests
import time


def check_api_health(base_url: str, timeout: int = 60) -> dict:
    endpoints = [
        "top_sectors",
        "top_netforeign",
        "top_interested_stocks",
        "khoi_tu_doanh",
        "khoi_ngoai",
        "index_summary",
        "index_fluctuation",
    ]

    health_status = {
        "healthy": True,
        "timestamp": time.time(),
        "endpoints": {},
        "total_endpoints": len(endpoints),
        "healthy_endpoints": 0,
    }

    for endpoint in endpoints:
        url = f"{base_url}/{endpoint}"
        try:
            response = requests.get(url, timeout=timeout)
            is_healthy = response.status_code == 200
            health_status["endpoints"][endpoint] = {
                "status": "healthy" if is_healthy else "unhealthy",
                "status_code": response.status_code,
                "response_time": response.elapsed.total_seconds(),
            }
            if is_healthy:
                health_status["healthy_endpoints"] += 1
        except Exception as e:
            health_status["endpoints"][endpoint] = {
                "status": "error",
                "error": str(e),
                "response_time": timeout,
            }

    health_status["health_percentage"] = (
        health_status["healthy_endpoints"] / health_status["total_endpoints"]
    ) * 100

    print(
        f"API Health Check: {health_status['healthy_endpoints']}/{health_status['total_endpoints']} endpoints healthy"
    )

    health_status["healthy"] = (
        health_status["healthy_endpoints"] == health_status["total_endpoints"]
    )

    return health_status


def main(base_url: str = "http://172.18.0.10:8000"):
    return check_api_health(base_url)
