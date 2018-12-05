import os
from shutil import copy
from distutils.core import setup
from setuptools.command.install import install


class PostInstallCommand(install):
      def run(self):
            if not os.path.exists('/etc/influxdbsyncer/influxdbsyncer.conf'):
                  config_dir = '/etc/influxdbsyncer'
                  if not os.path.exists(config_dir):
                        os.makedirs(config_dir)
                  import influxdbsyncer
                  config_file = os.path.join(os.path.dirname(influxdbsyncer.__file__), 'influxdbsyncer.conf')
                  copy(config_file, config_dir)
            install.run(self)


setup(name='influxdbsyncer',
      version='0.1',
      description='Sync missing datapoints from remote to local InfluxDB',
      url='https://github.com/alexpekurovsky/InfluxDBSyncer',
      author='Alex Pekurovsky',
      author_email='alex.pekurovsky@gmail.com',
      packages=['influxdbsyncer'],
      include_package_data=True,
      scripts=['bin/influxdbsyncer'],
      cmdclass={
            'install': PostInstallCommand
      },
      install_requires=[
            'schedule',
            'influxdb',
            'pandas',
            'numpy'
      ]
)