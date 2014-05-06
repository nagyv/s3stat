from setuptools import setup, find_packages
import s3stat

setup(
    name="s3stat",
    version="1.0.0",
    description='An extensible Amazon S3 and Cloudfront log parser.',
    long_description=s3stat.__doc__,
    author="Viktor Nagy",
    author_email='v@pulilab.com',
    url='https://github.com/nagyv/s3stat',
    include_package_data=True,
    zip_safe=False,
    install_requires=['boto'],
    py_modules=['s3stat'],
    scripts=['s3stat.py']
    # tests_require=['pytest'],
    # cmdclass = {
    #     'test': PyTest,
    # }
)