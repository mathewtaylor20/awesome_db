from setuptools import setup

setup(name='awesome_db',
      version='0.1',
      description='This db is awesome',
      url='https://github.com/mathewtaylor20/awesome_db.git',
      author='Mathew Taylor',
      author_email='mathewtaylor20@gmail.co.uk',
      license='',
      packages=['awesome_db'],
      entry_points={
          'console_scripts': [
              'awesome_db = awesome_db.__main__:main'
      ]},
      zip_safe=False)
