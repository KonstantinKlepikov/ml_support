from setuptools import setup
from os.path import join, dirname
import ml_support


setup(name='ml_support',
      version = ml_support.__version__,
      description = 'Some useful engine for ML research',
      long_description = open(join(dirname(__file__), 'README.rst')).read(),
      packages = ['ml_support'],
      author = 'Konstantin Klepikov',
      author_email = 'oformleno@gmail.com',
      download_url = 'https://github.com/KonstantinKlepikov/ml_support',
      license = 'GPL-3.0',
      zip_safe = False)