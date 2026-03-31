import os
import sys
from azure.identity import DefaultAzureCredential
from azure.purview.scanning import PurviewScanningClient
from azure.core.exceptions import HttpResponseError

#Config
PURVIEW_ACCOUNT_NAME = os.getenv("PURVIEW_ACCOUNT_NAME")
SCAN_ENDPOINT = f"https://{PURVIEW_ACCOUNT_NAME}.purview.azure.com"
DATALAKE_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
RESOURCE_GROUP = os.getenv("RESOURCE_GROUP")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")

def run_purview_sec_ops():
    #Authenticate using Managed Identity or Service Principal
    credential = DefaultAzureCredential()
    client = PurviewScanningClient(endpoint=SCAN_ENDPOINT, credential=credential)

    datasource_name = f"ds-{DATALAKE_NAME}"
    
    #Register the Data Source
    print(f"--- Registering Data Source: {datasource_name} ---")
    ds_payload = {
        "kind": "AdlsGen2",
        "properties": {
            "endpoint": f"https://{DATALAKE_NAME}.dfs.core.windows.net",
            "subscriptionId": SUBSCRIPTION_ID,
            "resourceGroup": RESOURCE_GROUP,
            "resourceName": DATALAKE_NAME
        }
    }

    try:
        client.data_sources.create_or_update(datasource_name, ds_payload)
        print("Successfully registered data source.")
    except HttpResponseError as e:
        print(f"Failed to register source: {e.message}")
        sys.exit(1)

    #Setup and Trigger Security Scan
    scan_name = f"security-scan-{DATALAKE_NAME}"
    scan_payload = {
        "kind": "AdlsGen2Msi",
        "properties": {
            "scanRulesetName": "AdlsGen2", # Default or custom security ruleset
            "scanRulesetType": "System"
        }
    }

    print(f"--- Triggering Security Scan: {scan_name} ---")
    try:
        client.scans.create_or_update(datasource_name, scan_name, scan_payload)
        client.scan_runs.run_scan(datasource_name, scan_name)
        print(f"Scan triggered successfully. Monitor progress in Purview Portal.")
    except HttpResponseError as e:
        print(f"Failed to trigger scan: {e.message}")
        sys.exit(1)

if __name__ == "__main__":
    run_purview_sec_ops()
