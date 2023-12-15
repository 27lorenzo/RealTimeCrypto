import configparser
import os, sys
#os.chdir(os.path.dirname(__file__))
os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))

class config(object):
    def __init__ (self):
        self.config = configparser.ConfigParser()
        self.config.sections()
        self.config.read('config.ini')

        self.config_hidden = configparser.ConfigParser()
        self.config_hidden.sections()
        self.config_hidden.read('config_hidden.ini')


    def readh(self, section, field):
        if field:
            return self.config_hidden[section].get(field)


    def read(self, section, field):
        if field:
            return self.config[section].get(field)

    def get_weight_severity(self, find):
        r = self.read('Severity Weight', find)
        if (r):
            return r
        else:
            return 1 # default value
