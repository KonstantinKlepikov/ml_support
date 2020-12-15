import os
import matplotlib.pyplot as plt
import core_paths


def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=300, path=core_paths.IMAGES_PATH):
    """Save ploting by matplotlib images
    
    param fig_id:
        Name of saving image
        str: 
    
    param tight_layout:
        bool: default True

    param fig_extension:
        Extension of image
        str: default png

    param resolution:
        Dpi resolution of saved image
        int: default 300

    path:
        Path to image folder
        str: default IMAGES_PATH
    """
    path = os.path.join(path, fig_id + '.' + fig_extension)
    print("Saving figure", fig_id)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)


if __name__ == "__main__":

    plt.plot([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
    save_fig('example', path=core_paths.IMAGES_PATH_TEST)
    print('save_fig .... ok')