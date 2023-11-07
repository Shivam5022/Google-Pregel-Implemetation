import shutil
import os

folder_path = 'checkpoint'
# Check if the folder exists
if os.path.exists(folder_path) and os.path.isdir(folder_path):
    try:
        shutil.rmtree(folder_path)
        os.makedirs(folder_path)
        print("Folder clearing done!!")
    except Exception as e:
        print(f"Failed to delete folder contents: {e}")
else:
<<<<<<< Updated upstream
    print("Folder does not exist or is not a directory.")
=======
    print("Folder does not exist or is not a directory.")
>>>>>>> Stashed changes
