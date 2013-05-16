# -*- coding: utf-8 -*-

import calendar
import datetime as dt
import locale
import os
import shutil


def moveSpmOutFiles(inDir, outPath, spmPrefixes, outDir=None, ext='.nii'):
  for i in range(0, len(spmPrefixes)):
    if(spmPrefixes[i].startswith("""'""")):
      spmPrefixes[i] = spmPrefixes[i][1:-1]
  for root, _dirs, filenames in os.walk(inDir):
    for f in filenames:
      goodPrefix = False;
      for spmPrefix in spmPrefixes:
        if(f.startswith(spmPrefix)):
          goodPrefix = True;
          break
      goodExtension = f.endswith(ext) or f.endswith('.txt')
      if (goodPrefix and goodExtension):
        if(outPath is not None):
          movePath(root + '/' + f, outPath)
        else:
          movePath(root + '/' + f, outDir + '/' + f)          
    
def movePathToDiskItem(srcPath, dstDI):
  if(dstDI is not None):
    return movePath(srcPath, dstDI.fullPath())

def movePath(srcPath, dstPath):
  if (os.path.exists(srcPath)):
    if(os.path.exists(dstPath)):      
      os.remove(dstPath) # do not use directly os.rename (but remove before) because : on windows, rename with dstPath already exists causes exception
    shutil.move(srcPath, dstPath) # shutil.move is better than os.rename, because os.rename failed if src and dst are not on the same filesystem
  if (os.path.exists(srcPath)):
    os.remove(srcPath)

def spm_today():  
  now = dt.datetime.now()
  
  currentLocale = locale.getlocale(locale.LC_TIME)  
  #locale.setlocale(locale.LC_TIME, ("en","us"))# mika : doesn't works for me
  locale.setlocale(locale.LC_TIME, ('en_US', 'UTF8'))
  month_name = calendar.month_name[now.month]
  locale.setlocale(locale.LC_TIME, currentLocale)
  
  month = month_name[:3]
  mth = month[0].upper() + month[1:]
  mth = mth.replace('é', 'e')
  mth = mth.replace('û', 'u')
  spm_today = str(now.day)
  if (now.day < 10):
    spm_today = '0' + str(now.day)
  d = str(now.year) + mth + spm_today
  return d