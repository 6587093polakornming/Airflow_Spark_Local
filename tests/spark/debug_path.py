import sys
import os

# Point to folder containing spark/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../spark')))

try:
    import app.utlis.clean_function
    print("✅ SUCCESS: Imported spark.app.utlis.clean_function")
except Exception as e:
    print("❌ FAILED to import spark.app.utlis.clean_function:")
    print("   ", e)