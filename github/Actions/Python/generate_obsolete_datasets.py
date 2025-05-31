import os
import json
import subprocess

FOLDER = "dataset"
OUTPUT_FILE = "obsolete_datasets.txt"
resource_group = os.getenv("AZURE_RESOURCE_GROUP")
data_factory_name = os.getenv("AZURE_ADF_NAME")

def get_repo_objects():
    return set(os.path.splitext(f)[0] for f in os.listdir(FOLDER) if f.endswith(".json"))

def get_live_objects():
    try:
        result = subprocess.run(
            [
                "az", "datafactory", "dataset", "list",
                "--resource-group", resource_group,
                "--factory-name", data_factory_name
            ],
            capture_output=True,
            text=True,
            check=True
        )
        objects = json.loads(result.stdout)
        return set(o["name"] for o in objects)
    except subprocess.CalledProcessError:
        print("Warning: Failed to fetch live dataset(s), assuming none exist.")
        return set()

def write_obsolete_objects(obsolete):
    with open(OUTPUT_FILE, "w") as f:
        for name in sorted(obsolete):
            f.write(f"{name}\n")

if __name__ == "__main__":
    repo = get_repo_objects()
    live = get_live_objects()
    obsolete = live - repo
    write_obsolete_objects(obsolete)
    print(f"Identified {len(obsolete)} obsolete dataset(s).")
