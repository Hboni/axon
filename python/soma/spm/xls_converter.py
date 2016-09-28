# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 11:20:25 2016

@author: ml236783
"""
import os
import xlrd
import xlwt
from collections import deque


class XlsConverter():
  """
  This converter class convert JSON dictionary to XLS file with some
  specificity, it mostly use in Nuclear Imaging - Axon toolbox.
  For XLS files generated by it, this class could reverse operation to
  get back JSON dictionary
  """

  def __init__(self, separator=';'):
    self.separator = ';'
    self.sheet_dict_deque = deque()

  def addDictToConvert(self, sheet_dict):
    """
    pydict =
    {
    "sheet_name":"",
    "row_header":"",#each column name separated by separator
    "column_values":{},
    }
    """
    self.sheet_dict_deque.append(sheet_dict)

  def run(self, xls_path):
    """
    Convert all dict in memory to xls file
    """
    if not self.sheet_dict_deque:
      raise ValueError("At least one sheet_dict is required")
    else:
      book = xlwt.Workbook(style_compression=2)
      for sheet_dict in self.sheet_dict_deque:
        sheet = book.add_sheet(sheet_dict["sheet_name"])
        column_values_dict = sheet_dict["column_values"]
        max_header_level = self._extractHowManyHeaderLevelAtMaximum(column_values_dict)
        if max_header_level == 0:
          continue
          print("no data found for %s" % sheet_dict["sheet_name"])
        else:
          end_header_row_index = max_header_level - 1
          self._writeRowHeader(sheet, sheet_dict["row_header"], end_header_row_index)
          header_dict = {}
          column_header_dict = self._extractHeaderDict(column_values_dict, header_dict)
          self._writeColumnHeader(sheet, sheet_dict["row_header"], column_header_dict, end_header_row_index)
          self._writeData(sheet, column_values_dict, column_header_dict, end_header_row_index)


      if not os.path.exists(os.path.dirname(xls_path)):
        os.makedirs(os.path.dirname(xls_path))
      book.save(xls_path)
      self.sheet_dict_deque.clear()

  def clearDictInMemory(self):
    """
    clear all dict in memory
    """
    self.sheet_dict_deque.clear()

  def _extractHowManyHeaderLevelAtMaximum(self, column_values_dict, current_level=-1, max_level=0):
    """
    current_level=-1, because first key is not header is row_ID
    """
    if isinstance(column_values_dict, dict):
      for value in column_values_dict.values():
        max_level_found = self._extractHowManyHeaderLevelAtMaximum(value, current_level+1, max_level)
        if max_level_found > max_level:
          max_level = max_level_found
      return max_level
    else:
      return current_level

  def _writeRowHeader(self, sheet, row_header, end_header_row_index):
    """
    write header above row id specify by keys from column_values
    """
    cell_style = xlwt.easyxf("align: wrap on, vert centre, horiz center")
    header_list = row_header.split(self.separator)
    for column_index, header_name in enumerate(header_list):
      sheet.merge(0, end_header_row_index, column_index, column_index)
      sheet.write(0, column_index, header_name, cell_style)

  def _extractHeaderDict(self, column_values_dict, header_dict, first_key=True):
    """
    extract all keys in value dict from column_values
    """
    for key, value in sorted(column_values_dict.items()):
      if first_key:
        header_dict = self._mergeDict(header_dict,
                                     self._extractHeaderDict(value,
                                                             header_dict,
                                                             False),
                                     d2_erase_d1=True)
      elif isinstance(value, dict):
        if not key in header_dict.keys():
          header_dict[key] = {}
        header_dict[key] = self._mergeDict(header_dict[key],
                                          self._extractHeaderDict(value,
                                                                  header_dict[key],
                                                                  False),
                                          d2_erase_d1=True)

      else:
        if not key in header_dict.keys():
          header_dict[key] = None
        pass
    return header_dict

  def _writeColumnHeader(self, sheet, row_header, column_header_dict, end_header_row_index):
    """
    write header above data in final value from column_values
    """
    current_column_index = len(row_header.split(self.separator))
    self._completeColumnHeader(sheet, end_header_row_index, column_header_dict, 0, current_column_index)

  def _completeColumnHeader(self, sheet, end_header_row_index, header_dict, current_row_index, current_column_index):
    cell_style = xlwt.easyxf("align: wrap on, vert centre, horiz center")
    for header_title, value in sorted(header_dict.items()):
      if value is not None:
        sheet.merge(current_row_index, current_row_index, current_column_index, current_column_index+self._countLeaves(value)-1)
        sheet.write(current_row_index, current_column_index, header_title, cell_style)
        self._completeColumnHeader(sheet, end_header_row_index, value, current_row_index+1, current_column_index)
        current_column_index += self._countLeaves(value)
      else:
        sheet.merge(current_row_index, end_header_row_index, current_column_index, current_column_index)
        sheet.write(current_row_index, current_column_index, header_title, cell_style)
        current_column_index += 1

  def _countLeaves(self, pydict, leaves=0):
    """
    count how many key levels are in pydict
    """
    for key, value in pydict.items():
      leaves += 1
      if isinstance(value, dict):
        leaves = self._countLeaves(value, leaves-1)
      else:
        pass
    return leaves

  def _writeData(self, sheet, column_values_dict, column_header_dict, end_header_row_index):
    """
    write data below headers
    """
    cell_style = xlwt.easyxf("pattern: pattern solid, fore_colour gray25;"
                             "align: wrap on, vert centre, horiz center;")
    current_row_index = end_header_row_index+1
    for row, row_dict in sorted(column_values_dict.items()):
      row_header_list = row.split(self.separator)
      for current_column_index, row_header in enumerate(row_header_list):
        sheet.write(current_row_index, current_column_index, row_header, cell_style)
      self._writeRowData(sheet, row_dict, column_header_dict, current_row_index, current_column_index+1)
      current_row_index += 1

  def _writeRowData(self, sheet, row_dict, column_header_dict, current_row_index, current_column_index):
    cell_style = xlwt.easyxf("align: wrap on, vert centre, horiz center")
    for key, value in sorted(column_header_dict.items()):
      if key in row_dict.keys():
        if isinstance(value, dict):
          current_column_index = self._writeRowData(sheet, row_dict[key], column_header_dict[key], current_row_index, current_column_index)
        else:
          sheet.write(current_row_index, current_column_index, row_dict[key], cell_style)
          current_column_index += 1
      else:
        if isinstance(value, dict):
          current_column_index += self._countLeaves(value)
        else:
          current_column_index += 1
    return current_column_index

  def _mergeDict(self, d1, d2, d2_erase_d1=False ):
    """This method allow to merge two python dictionaries
    without erase the deep keys, contrary to classical "update" method"""
    dict_merged = {}
    key_list = []
    key_list.extend( d1.keys() )
    key_list.extend( d2.keys() )
    key_list = list( set( key_list ) )
    for key in key_list:
      if isinstance( key, str ):
        new_key = key.replace( '\n', '' )
      else:
        new_key = key
      if key in d1.keys() and key in d2.keys():
        if isinstance( d1[key], dict ) and isinstance( d2[key], dict ):
          dict_merged[new_key] = self._mergeDict( d1[key], d2[key], d2_erase_d1 )
        else:
          # If same data are merged, neither d1 value nor d2 value was wrote
          if d2_erase_d1:
            dict_merged[new_key] = d2[key]
          else:
            dict_merged[new_key] = '** data merged **'
      elif key in d1.keys() and not key in d2.keys():
        dict_merged[new_key] = d1[key]
      elif not key in d1.keys() and key in d2.keys():
        dict_merged[new_key] = d2[key]
    return dict_merged


  def reverse(self, xls_path):
    """
    this method recompute sheet_dict list from xls created by it
    """
    sheet_dict_deque = deque()
    workbook = xlrd.open_workbook(xls_path)
    sheet_names = workbook.sheet_names()
    for sheet_name in sheet_names:
      sheet_dict = {}
      sheet = workbook.sheet_by_name(sheet_name)
      sheet_dict["sheet_name"] = sheet.name
      sheet_dict["row_header"] = self._getRowHeaderFromSheet(sheet)
      sheet_dict["column_values"] = self._getColumnValuesFromSheet(sheet)
      sheet_dict_deque.append(sheet_dict)
    return sheet_dict_deque

  def _getRowHeaderFromSheet(self, sheet):
    """
    first row contains header and second is empty, if it is not the case it will do not work
    """
    row_header_list = []
    column_index = 0
    while(sheet.cell_type(1, column_index) in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
      row_header_list.append(sheet.cell_value(0, column_index))
      column_index += 1
    return self.separator.join(row_header_list)

  def _getColumnValuesFromSheet(self, sheet):
    """
    extract each row value with all column header above
    """
    first_row_data_index, first_column_data_index = self._getFirstCellIndexWithData(sheet)
    row_data_index = first_row_data_index
    column_values_dict = {}
    while(sheet.cell_type(row_data_index, 0) not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
      row_id = self._getRowId(sheet, row_data_index, first_column_data_index)
      column_values_dict[row_id] = self._getValues(sheet, row_data_index, first_row_data_index, first_column_data_index)
      row_data_index += 1
    return column_values_dict

  def _getFirstCellIndexWithData(self, sheet):
    """
    extract position of first cell wich is not header
    """
    row_index = 1
    while(sheet.cell_type(row_index, 0) in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
      row_index += 1
    column_index = 0
    while(sheet.cell_type(1, column_index) in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
      column_index += 1
    return row_index, column_index

  def _getRowId(self, sheet, row_data_index, first_column_data_index):
    """
    get and join all cells about row_id
    """
    row_id_list = []
    for column_index in range(first_column_data_index):
      row_id_list.append(str(sheet.cell_value(row_data_index,column_index )))
    return self.separator.join(row_id_list)

  def _getValues(self, sheet, row_data_index, first_row_data_index, column_data_index):
    """
    extract row value with all column header above
    """
    values_dict = {}
    column_index = column_data_index
    while(column_index < sheet.ncols):
      #iter on column until all row header are empty (end of header)
      val = sheet.cell_value(row_data_index, column_index)
      for row_index in range(first_row_data_index, 0, -1):
        tmp_dict = {self._getUpperHeaderWord(sheet, row_index, column_index):val}
        val = tmp_dict
      values_dict = self._mergeDict(values_dict, tmp_dict)
      column_index += 1
    return values_dict

  def _getUpperHeaderWord(self, sheet, row_index, column_index):
    """
        h1      |h2
    sh1  |sh2   |sh1
    if column_index = 0 and row_index = 1, return [h1, sh1]
    if column_index = 1 and row_index = 1, return [h1, sh2]
    """
    row_of_interest = row_index-1
    while(sheet.cell_type(row_of_interest, column_index) in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK)):
      column_index -= 1
    return sheet.cell_value(row_of_interest, column_index)