import os


PROJECT_ROOT_DIR = '.'
IMAGES_PATH = os.path.join(PROJECT_ROOT_DIR, 'images', 'exitimg')

DATA_PATH = os.path.join('../../', 'input')
DATA_PATH_TEST = os.path.join('../../', 'test')
DATA_OUTPUT_TEST = 'subtitanic'

DUMP_PATH = os.path.join(PROJECT_ROOT_DIR, 'kernels', 'dumped_data')

if __name__ == "__main__":
    
    print(IMAGES_PATH)
    print(DATA_PATH)