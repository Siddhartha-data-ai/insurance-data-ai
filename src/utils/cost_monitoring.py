"""
Cost Monitoring and Optimization
Track and optimize Databricks compute and storage costs
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from decimal import Decimal
import json


class CostMonitor:
    """
    Monitor and track costs for Databricks resources
    Helps identify cost optimization opportunities
    """

    def __init__(self):
        self.cost_records: List[Dict[str, Any]] = []
        self.optimization_recommendations: List[Dict[str, str]] = []

    def record_cluster_cost(
        self,
        cluster_id: str,
        cluster_name: str,
        node_type: str,
        num_workers: int,
        runtime_minutes: float,
        dbu_rate: float = 0.15,  # DBU rate in USD
        compute_rate: float = 0.50,  # Compute rate per node-hour
    ):
        """
        Record cluster compute cost

        Args:
            cluster_id: Cluster ID
            cluster_name: Cluster name
            node_type: Instance type (e.g., i3.xlarge)
            num_workers: Number of worker nodes
            runtime_minutes: Runtime in minutes
            dbu_rate: DBU cost per unit
            compute_rate: Compute cost per node per hour
        """
        runtime_hours = runtime_minutes / 60
        total_nodes = num_workers + 1  # Workers + driver

        # Estimate DBU consumption (simplified)
        dbu_consumption = runtime_hours * num_workers * 2  # 2 DBUs per worker-hour estimate
        dbu_cost = dbu_consumption * dbu_rate

        # Compute cost
        compute_cost = runtime_hours * total_nodes * compute_rate

        total_cost = dbu_cost + compute_cost

        self.cost_records.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "resource_type": "cluster",
                "cluster_id": cluster_id,
                "cluster_name": cluster_name,
                "node_type": node_type,
                "num_workers": num_workers,
                "runtime_hours": round(runtime_hours, 2),
                "dbu_consumption": round(dbu_consumption, 2),
                "dbu_cost_usd": round(dbu_cost, 2),
                "compute_cost_usd": round(compute_cost, 2),
                "total_cost_usd": round(total_cost, 2),
            }
        )

        # Generate optimization recommendations
        if runtime_minutes < 10:
            self.optimization_recommendations.append(
                {
                    "resource": cluster_name,
                    "issue": "Short-lived cluster",
                    "recommendation": "Consider using shared compute or serverless for short jobs",
                    "potential_savings": "20-40%",
                }
            )

        if num_workers > 10:
            self.optimization_recommendations.append(
                {
                    "resource": cluster_name,
                    "issue": "Large cluster size",
                    "recommendation": "Enable autoscaling to optimize worker count",
                    "potential_savings": "15-30%",
                }
            )

    def record_storage_cost(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        size_gb: float,
        storage_rate_per_gb: float = 0.023,  # S3 standard storage rate
    ):
        """
        Record storage cost for Delta tables

        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            table_name: Table name
            size_gb: Size in GB
            storage_rate_per_gb: Storage cost per GB per month
        """
        monthly_cost = size_gb * storage_rate_per_gb

        self.cost_records.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "resource_type": "storage",
                "catalog": catalog_name,
                "schema": schema_name,
                "table": table_name,
                "size_gb": round(size_gb, 2),
                "monthly_cost_usd": round(monthly_cost, 2),
            }
        )

        # Optimization recommendations
        if size_gb > 1000:  # Large tables
            self.optimization_recommendations.append(
                {
                    "resource": f"{catalog_name}.{schema_name}.{table_name}",
                    "issue": "Large table size",
                    "recommendation": "Enable table optimization, z-ordering, and vacuum old files",
                    "potential_savings": "10-25%",
                }
            )

    def record_query_cost(
        self,
        query_id: str,
        query_name: str,
        execution_time_seconds: float,
        data_scanned_gb: float,
        warehouse_size: str = "Small",
    ):
        """
        Record cost for SQL warehouse query

        Args:
            query_id: Query ID
            query_name: Query name/description
            execution_time_seconds: Query execution time
            data_scanned_gb: Amount of data scanned
            warehouse_size: SQL warehouse size (Small, Medium, Large)
        """
        # SQL Warehouse DBU rates (approximate)
        warehouse_dbu_rates = {
            "Small": 0.22,  # DBU per hour
            "Medium": 0.44,
            "Large": 0.88,
            "X-Large": 1.76,
        }

        dbu_rate_per_hour = warehouse_dbu_rates.get(warehouse_size, 0.22)
        execution_hours = execution_time_seconds / 3600

        dbu_consumption = execution_hours * dbu_rate_per_hour
        query_cost = dbu_consumption * 0.15  # $0.15 per DBU

        self.cost_records.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "resource_type": "sql_warehouse_query",
                "query_id": query_id,
                "query_name": query_name,
                "warehouse_size": warehouse_size,
                "execution_time_seconds": round(execution_time_seconds, 2),
                "data_scanned_gb": round(data_scanned_gb, 2),
                "dbu_consumption": round(dbu_consumption, 4),
                "cost_usd": round(query_cost, 4),
            }
        )

        # Optimization recommendations
        if data_scanned_gb > 100:
            self.optimization_recommendations.append(
                {
                    "resource": query_name,
                    "issue": "High data scan volume",
                    "recommendation": "Add partitioning/z-ordering, use incremental processing",
                    "potential_savings": "30-50%",
                }
            )

        if execution_time_seconds > 600:  # Queries over 10 minutes
            self.optimization_recommendations.append(
                {
                    "resource": query_name,
                    "issue": "Long-running query",
                    "recommendation": "Optimize query logic, add caching, or use materialized views",
                    "potential_savings": "20-40%",
                }
            )

    def get_cost_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Get cost summary for specified period

        Args:
            days: Number of days to include in summary

        Returns:
            Cost summary dictionary
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        recent_costs = [
            record
            for record in self.cost_records
            if datetime.fromisoformat(record["timestamp"].replace("Z", "")) >= cutoff_date
        ]

        if not recent_costs:
            return {"message": "No cost data available"}

        # Calculate totals by resource type
        cost_by_type = {}
        for record in recent_costs:
            resource_type = record["resource_type"]
            if resource_type not in cost_by_type:
                cost_by_type[resource_type] = {"count": 0, "total_cost_usd": 0}

            cost_by_type[resource_type]["count"] += 1

            if resource_type == "cluster":
                cost_by_type[resource_type]["total_cost_usd"] += record["total_cost_usd"]
            elif resource_type == "storage":
                cost_by_type[resource_type]["total_cost_usd"] += record["monthly_cost_usd"]
            elif resource_type == "sql_warehouse_query":
                cost_by_type[resource_type]["total_cost_usd"] += record["cost_usd"]

        total_cost = sum(item["total_cost_usd"] for item in cost_by_type.values())

        return {
            "period_days": days,
            "total_cost_usd": round(total_cost, 2),
            "cost_by_resource_type": {
                k: {**v, "total_cost_usd": round(v["total_cost_usd"], 2)}
                for k, v in cost_by_type.items()
            },
            "estimated_monthly_cost_usd": round(total_cost * (30 / days), 2),
            "record_count": len(recent_costs),
        }

    def get_optimization_recommendations(
        self, limit: int = 10
    ) -> List[Dict[str, str]]:
        """
        Get top cost optimization recommendations

        Args:
            limit: Maximum number of recommendations to return

        Returns:
            List of optimization recommendations
        """
        # Deduplicate recommendations
        unique_recommendations = []
        seen = set()

        for rec in self.optimization_recommendations:
            key = (rec["resource"], rec["issue"])
            if key not in seen:
                unique_recommendations.append(rec)
                seen.add(key)

        return unique_recommendations[:limit]

    def estimate_savings(self) -> Dict[str, Any]:
        """
        Estimate potential cost savings from optimization

        Returns:
            Savings estimate dictionary
        """
        recommendations = self.get_optimization_recommendations(limit=100)

        # Parse savings percentages and calculate potential
        total_cost = self.get_cost_summary()["total_cost_usd"]

        # Conservative estimate: average 20% savings potential
        estimated_savings_percentage = 0.20
        estimated_monthly_savings = total_cost * estimated_savings_percentage

        return {
            "current_monthly_cost_usd": round(total_cost, 2),
            "total_recommendations": len(recommendations),
            "estimated_savings_percentage": round(estimated_savings_percentage * 100, 1),
            "estimated_monthly_savings_usd": round(estimated_monthly_savings, 2),
            "estimated_annual_savings_usd": round(estimated_monthly_savings * 12, 2),
            "top_opportunities": recommendations[:5],
        }

    def export_cost_report(self) -> str:
        """Export cost report as JSON"""
        report = {
            "report_date": datetime.utcnow().isoformat(),
            "summary": self.get_cost_summary(),
            "optimization_recommendations": self.get_optimization_recommendations(),
            "savings_estimate": self.estimate_savings(),
            "detailed_records": self.cost_records[-100:],  # Last 100 records
        }
        return json.dumps(report, indent=2)


# Global cost monitor instance
_cost_monitor = CostMonitor()


def get_cost_monitor() -> CostMonitor:
    """Get global cost monitor instance"""
    return _cost_monitor


# Example usage
if __name__ == "__main__":
    monitor = get_cost_monitor()

    # Record cluster costs
    monitor.record_cluster_cost(
        cluster_id="1234-567890-abc123",
        cluster_name="etl_cluster",
        node_type="i3.xlarge",
        num_workers=4,
        runtime_minutes=45.5,
    )

    monitor.record_cluster_cost(
        cluster_id="1234-567890-def456",
        cluster_name="ml_training_cluster",
        node_type="i3.2xlarge",
        num_workers=8,
        runtime_minutes=120.0,
    )

    # Record storage costs
    monitor.record_storage_cost(
        catalog_name="insurance_prod",
        schema_name="bronze",
        table_name="customers_raw",
        size_gb=1500.5,
    )

    monitor.record_storage_cost(
        catalog_name="insurance_prod",
        schema_name="silver",
        table_name="claims_fact",
        size_gb=3200.8,
    )

    # Record query costs
    monitor.record_query_cost(
        query_id="query-001",
        query_name="daily_claims_report",
        execution_time_seconds=125.5,
        data_scanned_gb=250.0,
        warehouse_size="Medium",
    )

    # Generate report
    print("=== COST SUMMARY ===")
    print(json.dumps(monitor.get_cost_summary(), indent=2))

    print("\n=== OPTIMIZATION RECOMMENDATIONS ===")
    print(json.dumps(monitor.get_optimization_recommendations(), indent=2))

    print("\n=== SAVINGS ESTIMATE ===")
    print(json.dumps(monitor.estimate_savings(), indent=2))

