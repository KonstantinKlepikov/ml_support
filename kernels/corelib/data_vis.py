import os
import matplotlib.pyplot as plt

PROJECT_ROOT_DIR = "."
CHAPTER_ID = "exitimg"
IMAGES_PATH = os.path.join(PROJECT_ROOT_DIR, "images", CHAPTER_ID)

def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=300):

    """
    Save ploting by matplotlib images
    
    Parameters
    ----------
    :param fig_id: name of saving image
        str
    
    :param tight_layout:
        default True

    :param fig_extension: extension of image
        str, default png

    :param resolution: dpi resolution of saved image
        int, default 300

    """
    
    path = os.path.join(IMAGES_PATH, fig_id + "." + fig_extension)
    print("Saving figure", fig_id)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)