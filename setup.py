try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    # Required meta-data:
    name='toron',
    version='0.0.1',
    url='https://github.com/shawnbrown/toron',
    packages=['toron'],
    # Additional fields:
    install_requires=['typing_extensions;python_version<"3.10"'],
    python_requires='>=3.7',
    description='',
    long_description='',
    author='Shawn Brown',
    author_email='shawnbrown@users.noreply.github.com',
    classifiers=['Development Status :: 1 - Planning'],
)

