import os, logging
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, metrics


## Application Insights Initalize
configure_azure_monitor(connection_string=os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"])
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
failsafe_counter = meter.create_counter(
    name="failsafe_runs",
    description="Number of failsafe subscription renewals",
    unit="1"
)
print("Telemetry Initialized")