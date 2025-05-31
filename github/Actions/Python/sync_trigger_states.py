import os
import json
import subprocess
import sys

TRIGGER_FOLDER = "trigger"
resource_group = os.getenv("AZURE_RESOURCE_GROUP")
data_factory_name = os.getenv("AZURE_ADF_NAME")

def trigger_exists(name):
    try:
        subprocess.run(
            [
                "az", "datafactory", "trigger", "show",
                "--resource-group", resource_group,
                "--factory-name", data_factory_name,
                "--name", name
            ],
            capture_output=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False

def run_az_command(action, trigger_name):
    try:
        subprocess.run(
            [
                "az", "datafactory", "trigger", action,
                "--resource-group", resource_group,
                "--factory-name", data_factory_name,
                "--name", trigger_name
            ],
            check=True
        )
        print(f"{action.capitalize()}ed trigger: {trigger_name}")
    except subprocess.CalledProcessError:
        print(f"Failed to {action} trigger: {trigger_name}")

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("dev", "prod"):
        print("Usage: python sync_trigger_states.py <dev|prod>")
        sys.exit(1)

    environment = sys.argv[1]

    for file in os.listdir(TRIGGER_FOLDER):
        if not file.endswith(".json"):
            continue

        with open(os.path.join(TRIGGER_FOLDER, file), "r", encoding="utf-8") as f:
            trigger = json.load(f)

        name = os.path.splitext(file)[0]
        lower_name = name.lower()
        state_in_git = trigger.get("properties", {}).get("runtimeState", "").lower()

        if not trigger_exists(name):
            print(f"Trigger {name} does not exist in ADF. Skipping.")
            continue

        # Dev logic
        if environment == "dev":
            if lower_name.startswith("prod_"):
                run_az_command("stop", name)
            elif lower_name.startswith("dev_") and state_in_git == "started":
                run_az_command("start", name)

        # Prod logic
        elif environment == "prod":
            if lower_name.startswith("dev_"):
                run_az_command("stop", name)
            elif lower_name.startswith("prod_"):
                run_az_command("start", name)

if __name__ == "__main__":
    main()
