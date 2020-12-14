import os

# main
PROJECT_ROOT_DIR = '../../'

DATA_PATH = os.path.join(PROJECT_ROOT_DIR, 'input')
DUMP_PATH = os.path.join(PROJECT_ROOT_DIR, 'kernels', 'dumped_data')
IMAGES_PATH = os.path.join(PROJECT_ROOT_DIR, 'kernels', 'images', 'exitimg')

# test
DATA_PATH_TEST = os.path.join('../', 'test')
DATA_OUTPUT_TEST = 'subtitanic'
DUMP_PATH_TEST = os.path.join('../', 'test', 'dumped_data')

if __name__ == "__main__":
    
    print(IMAGES_PATH)
    print(DATA_PATH)
    print(DUMP_PATH)

    print(DATA_PATH_TEST)
    print(DATA_OUTPUT_TEST)
    print(DUMP_PATH_TEST)