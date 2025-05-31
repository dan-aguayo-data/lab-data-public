import os
import json
import subprocess

# CONFIG
pipeline_folder = "pipeline"
output_file = "obsolete_pipelines.txt"
resource_group = os.getenv("AZURE_RESOURCE_GROUP")
data_factory_name = os.getenv("AZURE_ADF_NAME")

def get_repo_pipelines():
    pipelines = set()
    for file in os.listdir(pipeline_folder):
        if file.endswith(".json"):
            pipelines.add(os.path.splitext(file)[0])
    return pipelines

def get_live_pipelines():
    try:
        result = subprocess.run(
            [
                "az", "datafactory", "pipeline", "list",
                "--resource-group", resource_group,
                "--factory-name", data_factory_name
            ],
            capture_output=True,
            text=True,
            check=True
        )
        pipelines = json.loads(result.stdout)
        return set(p["name"] for p in pipelines)
    except subprocess.CalledProcessError as e:
        print("Warning: Failed to fetch live pipelines, assuming none exist.")
        return set()

def write_obsolete_pipelines(obsolete):
    with open(output_file, "w") as f:
        for name in sorted(obsolete):
            f.write(f"{name}\n")

if __name__ == "__main__":
    repo = get_repo_pipelines()
    live = get_live_pipelines()
    obsolete = live - repo
    write_obsolete_pipelines(obsolete)
    print(f"Identified {len(obsolete)} obsolete pipeline(s).")
