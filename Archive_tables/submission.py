# submission.py
from azure.ai.ml import MLClient, command
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import Environment

# Set up the ML client
subscription_id = "a5d4ffbe-d287-4dd1-86c9-f1214fe751d6"
resource_group = "CDIOUZIMA"
workspace_name = "UZIMA_ML_Workspace"

credential = DefaultAzureCredential()
ml_client = MLClient(credential, subscription_id, resource_group, workspace_name)

# Use a modern, stable AML base image so the conda bootstrap step succeeds
env = Environment(
    name="fitbit-data-backup-env",
    version="10",  # bump version
    description="Environment for backing up large Fitbit tables",
    image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    conda_file="Archive_tables/environment.yml",
)



# Create the command job
job = command(
    code="./Archive_tables",
    command="python backup_script.py",
    environment=env,  # Pass the environment object, not a string
    compute="compute-standard-E4ds-v4",
    display_name="fitbit-data-backup-job",
    experiment_name="fitbit-data-backup",
    description="Job to back up large Fitbit data tables to Azure Blob Storage",
    environment_variables={
        "DB_SERVER": "uzima-dmac.database.windows.net",
        "DB_NAME": "Uzima_db",
        "DB_USERNAME": "adminuzima",
        "DB_PASSWORD": "gVEVxxDag99!Z5^6hl4eB",
        "STORAGE_ACCOUNT_NAME": "uzimadmac",
        "STORAGE_CONTAINER": "hcw-fitbit-survey-datatsets",
        "STORAGE_SAS_TOKEN": "?sv=2024-11-04&ss=bfqt&srt=sco&sp=rwlacupx&se=2027-08-20T04:19:55Z&st=2025-08-19T20:04:55Z&spr=https,http&sig=%2FP4yKhZZ3HBpuQt2%2B9iwMA5D8LWgWUmbhzxDpv1f0mQ%3D"
    }
)

# Submit the job
print("Submitting job...")
returned_job = ml_client.jobs.create_or_update(job)

# Get workspace details
workspace = ml_client.workspaces.get(workspace_name)

# Construct the job URL
job_url = (
    f"https://ml.azure.com/experiments/fitbit-data-backup/runs/{returned_job.name}?wsid="
    f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/"
    f"providers/Microsoft.MachineLearningServices/workspaces/{workspace_name}"
)

print("\n" + "="*80)
print(f"✅ Job submitted successfully!")
print(f"🔗 Monitor your job here: {job_url}")
print("="*80 + "\n")