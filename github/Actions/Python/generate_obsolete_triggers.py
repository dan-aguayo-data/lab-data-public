import os
import json
import subprocess

TRIGGER_FOLDER = "trigger"
OUTPUT_FILE = "obsolete_triggers.txt"
resource_group = os.getenv("AZURE_RESOURCE_GROUP")
data_factory_name = os.getenv("AZURE_ADF_NAME")

def get_repo_triggers():
    return set(os.path.splitext(f)[0] for f in os.listdir(TRIGGER_FOLDER) if f.endswith(".json"))

def get_live_triggers():
    try:
        result = subprocess.run(
            [
                "az", "datafactory", "trigger", "list",
                "--resource-group", resource_group,
                "--factory-name", data_factory_name
            ],
            capture_output=True,
            text=True,
            check=True
        )
        triggers = json.loads(result.stdout)
        return set(t["name"] for t in triggers)
    except subprocess.CalledProcessError:
        print("Warning: Failed to list triggers, assuming none exist.")
        return set()

def write_obsolete_triggers(obsolete):
    with open(OUTPUT_FILE, "w") as f:
        for name in sorted(obsolete):
            f.write(f"{name}\n")

if __name__ == "__main__":
    repo = get_repo_triggers()
    live = get_live_triggers()
    obsolete = live - repo
    write_obsolete_triggers(obsolete)
    print(f"Found {len(obsolete)} obsolete trigger(s).")
