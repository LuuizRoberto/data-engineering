import base64
import boto3
from botocore.exceptions import ClientError
import collections
import concurrent.futures
import csv
import datetime as pydt
import json
import math
import os
import pprint
import pysftp
import pytz
import re
import requests
import shutil
import subprocess
import tempfile
import traceback
import unicodedata
import uuid
import zipfile
import time
import openpyxl
import gnupg
import chardet

from functools import wraps
from dateutil.relativedelta import relativedelta
from abc import ABC, abstractmethod

from google.auth import compute_engine
from google.cloud import datastore, storage, bigquery, secretmanager
from proto.datetime_helpers import DatetimeWithNanoseconds

from google.cloud.exceptions import NotFound
from google.cloud.datastore import Entity

import pyspark.sql.functions as sqlfunc
import pyspark.sql.types as sqltypes
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import to_json, to_date
from pyspark.sql.functions import when, lit

import simple_salesforce

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

# verifica se o pacote está instalado
if 'simple-salesforce' in subprocess.check_output("pip list", shell=True).decode():
    # realiza a instalação do pacote
    import simple_salesforce
else:
    print("simple-salesforce não está instalado")

if 'facebook_business' in subprocess.check_output("pip list", shell=True).decode():
    # realiza a instalação do pacote
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.adcreative import AdCreative
    from facebook_business.adobjects.ad import Ad
    from facebook_business.adobjects.adsinsights import AdsInsights
    from facebook_business.adobjects.adimage import AdImage
    from facebook_business.adobjects.adcreativeobjectstoryspec import AdCreativeObjectStorySpec
    from facebook_business.adobjects.adcreativelinkdata import AdCreativeLinkData
    from facebook_business.adobjects.adcreativevideodata import AdCreativeVideoData
    from facebook_business.adobjects.adcreativelinkdatacalltoaction import AdCreativeLinkDataCallToAction
    from facebook_business.adobjects.abstractobject import AbstractObject
    from facebook_business.api import FacebookAdsApi
else:
    print("facebook_business não está instalado")
