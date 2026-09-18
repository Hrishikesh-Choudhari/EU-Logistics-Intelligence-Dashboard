This project is a recreation of a live continuous improvement initiative I led at Amazon EU Logistics, originally delivered as an internal analytics product that identified £22M in labour cost saving opportunities and scaled to 1,500+ monthly active users across EU operations teams.

The original dashboard gave operations managers and site leads their first data-driven view of associate shift productivity — tracking how efficiently associates ramp up at shift start, return from breaks, and utilise time before shift end. Prior to this, there was no structured way to quantify where productive time was being lost or what it was costing the business.

This public version recreates the same logic and architecture using dummy data, built on Databricks Community Edition with a Bronze → Silver → Gold medallion pipeline in PySpark and SQL, with Power BI sitting on top of the gold layer.

The pipeline ingests from three simulated source systems — HR shift records, operations event logs, and country-level wages data — across 25 EU sites in 14 countries. The gold layer surfaces compliance rates for Fast Start, Post Break Ramp Up, and Strong Finish, with wasted labour cost quantified per metric, per process activity, and per site.

Built to demonstrate end-to-end data engineering and BI skills to the professional community.
