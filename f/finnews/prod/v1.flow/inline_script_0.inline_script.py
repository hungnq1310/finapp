def main(health_status: dict):
    print(
        f"API health check failed: {health_status.get('health_percentage', 0)}% of endpoints healthy"
    )
    print("Returning fallback empty data structure")

    return {
        "top_sectors": [],
        "top_netforeign": {"buy": [], "sell": []},
        "top_interested": [],
        "khoi_tu_doanh": 0,
        "khoi_ngoai": {"vol": 0, "net_value": 0},
        "index_summary": [],
        "impact_up": {"stock_code": [], "total": 0},
        "impact_down": {"stock_code": [], "total": 0},
        "success": False,
        "error": "API health check failed",
        "fallback_used": True,
    }
