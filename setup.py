from setuptools import setup, find_packages

setup(name='naverLand_v2',
      version='0.1',
      url='https://github.com/ajcltm/naverLand_v2',
      license='jnu',
      author='ajcltm',
      author_email='ajcltm@gmail.com',
      description='',
      packages=['scrap'],
      zip_safe=False,
      setup_requires=['requests>=1.0'],
      test_suite='test.test_guScraper')