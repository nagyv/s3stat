from setuptools import setup, find_packages
try:
    import s3stat
    doc  = s3stat.__doc__
except ImportError:
    doc = "The docs are only available when the package is already installed. Sorry for this."
    
setup(
    name="s3stat",
    version="2.1.0",
    description='An extensible Amazon S3 and Cloudfront log parser.',
    long_description=doc,
    author="Viktor Nagy",
    author_email='v@pulilab.com',
    url='https://github.com/nagyv/s3stat',
    include_package_data=True,
    zip_safe=False,
    install_requires=['boto', 'tempdir'],
    py_modules=['s3stat'],
    scripts=['s3stat.py'],
    keywords="s3stat amazon statistics goaccess"
    # tests_require=['pytest'],
    # cmdclass = {
    #     'test': PyTest,
    # }
)