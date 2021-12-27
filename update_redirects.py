#!/usr/bin/env python3
#
#  Copyright (c) 2021 unfoldingWord
#  http://creativecommons.org/licenses/MIT/
#  See LICENSE file for details.
#
#  Contributors:
#  Richard Mahn <rich.mahn@unfoldingword.org>

from app_settings.app_settings import AppSettings

AppSettings(prefix="")

if __name__ == '__main__':
  # objects = AppSettings.door43_s3_handler().get_objects("u/", suffix="/index.html")
  # print(len(list(objects)))
  result = AppSettings.door43_s3_handler().client.list_objects(Bucket=AppSettings.door43_s3_handler().bucket_name, Prefix="u/", Delimiter="/")
  for obj in result['CommonPrefixes']: 
    prefix = obj["Prefix"]
    print(prefix)
    result2 = AppSettings.door43_s3_handler().client.list_objects(Bucket=AppSettings.door43_s3_handler().bucket_name, Prefix=prefix, Delimiter="/")
    for obj2 in result2['CommonPrefixes']: 
      prefix2 = obj2["Prefix"].removesuffix('/')
      print(prefix2)
      master_exists = AppSettings.door43_s3_handler().object_exists(prefix2 + "/master/index.html")
      print(master_exists)
      if master_exists:
        location = '/' + prefix2 + "/master/"
        print(f"Redirecting {prefix2} to {location}")
        AppSettings.door43_s3_handler().redirect(key=prefix2, location=location)
        print(f"Redirecting {prefix2}/index.html to {location}")
        AppSettings.door43_s3_handler().redirect(key=prefix2 + '/index.html', location=location)
