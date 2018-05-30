import os
import subprocess
import sys

APP_FILE = "fake_consumer_service.py"

if os.path.exists(APP_FILE):
    app_path = "."
elif os.path.exists(os.path.join("tools", APP_FILE)):
    app_path = "tools"
else:
    app_path = os.path.dirname(os.path.realpath(__file__))

env_vars = os.environ.copy()
env_vars["FLASK_APP"] = os.path.join(app_path, APP_FILE)

subprocess.call(["flask", "run"] + sys.argv[1:], env=env_vars)
