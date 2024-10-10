"""
Copyright start
MIT License
Copyright (c) 2024 Fortinet Inc
Copyright end
"""
import json

from requests import request
from connectors.core.connector import get_logger, ConnectorError
from .google_api_auth import *
from .constants import *

SHEETS_API_VERSION = 'v4'

logger = get_logger('google-sheets')


def api_request(method, endpoint, connector_info, config, params=None, data=None, headers={}):
    try:
        go = GoogleAuth(config)
        endpoint = go.host + "/" + endpoint
        token = go.validate_token(config, connector_info)
        headers['Authorization'] = token
        headers['Content-Type'] = 'application/json'
        response = request(method, endpoint, headers=headers, params=params, data=data, verify=go.verify_ssl)
        if response.ok or response.status_code == 204:
            if 'json' in str(response.headers):
                return response.json()
            else:
                return response
        else:
            logger.error("{0}".format(response.status_code))
            raise ConnectorError("{0}:{1}".format(response.status_code, response.text))
    except requests.exceptions.SSLError:
        raise ConnectorError('SSL certificate validation failed')
    except requests.exceptions.ConnectTimeout:
        raise ConnectorError('The request timed out while trying to connect to the server')
    except requests.exceptions.ReadTimeout:
        raise ConnectorError(
            'The server did not send any data in the allotted amount of time')
    except requests.exceptions.ConnectionError:
        raise ConnectorError('Invalid Credentials')
    except Exception as err:
        raise ConnectorError(str(err))


def check_payload(payload):
    l = {}
    for k, v in payload.items():
        if isinstance(v, dict):
            x = check_payload(v)
            if len(x.keys()) > 0:
                l[k] = x
        elif isinstance(v, list):
            p = []
            for c in v:
                if isinstance(c, dict):
                    x = check_payload(c)
                    if len(x.keys()) > 0:
                        p.append(x)
                elif c is not None and c != '':
                    p.append(c)
            if p != []:
                l[k] = p
        elif v is not None and v != '':
            l[k] = v
    return l


def build_payload(payload):
    payload = {k: v for k, v in payload.items() if v is not None and v != ''}
    return payload


def create_spreadsheet(config, params, connector_info):
    try:
        url = '{0}/spreadsheets'.format(SHEETS_API_VERSION)
        payload = {
            'properties': {
                'title': params.get('title'),
                'locale': params.get('locale'),
                'autoRecalc': MAPPING.get(params.get('autoRecalc')) if params.get('autoRecalc') else '',
                'timeZone': params.get('timeZone'),
                'iterativeCalculationSettings': {
                    'maxIterations': params.get('maxIterations'),
                    'convergenceThreshold': params.get('convergenceThreshold')
                },
                'spreadsheetTheme': {
                    'primaryFontFamily': params.get('primaryFontFamily'),
                    'themeColors': params.get('themeColors')
                }
            },
            'sheets': params.get('sheets'),
            'namedRanges': params.get('namedRanges'),
            'developerMetadata': params.get('developerMetadata'),
            'dataSources': params.get('dataSources')
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def get_spreadsheet_details(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}'.format(SHEETS_API_VERSION, params.get('spreadsheetId'))
        ranges = params.get('ranges')
        if ranges:
            ranges = ranges if type(ranges) == list else ranges.split(",")
        query_parameter = {
            'ranges': ranges,
            'includeGridData': params.get('includeGridData')
        }
        query_parameter = build_payload(query_parameter)
        response = api_request('GET', url, connector_info, config, params=query_parameter)
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def filter_spreadsheet(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}:getByDataFilter'.format(SHEETS_API_VERSION, params.get('spreadsheetId'))
        payload = {
            'dataFilters': params.get('dataFilters'),
            'includeGridData': params.get('includeGridData')
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def add_row_to_spreadsheet(config, params, connector_info):
    try:
        payload = {}
        url = '{0}/spreadsheets/{1}/values/{2}:append'.format(SHEETS_API_VERSION, params.get('spreadsheetId'),
                                                              params.get('range'))
        query_parameter = {
            'valueInputOption': MAPPING.get(params.get('valueInputOption')) if params.get('valueInputOption') else '',
            'insertDataOption': MAPPING.get(params.get('insertDataOption')) if params.get('insertDataOption') else '',
            'includeValuesInResponse': params.get('includeValuesInResponse'),
            'responseValueRenderOption': MAPPING.get(params.get('responseValueRenderOption')) if params.get(
                'responseValueRenderOption') else '',
            'responseDateTimeRenderOption': MAPPING.get(params.get('responseDateTimeRenderOption')) if params.get(
                'responseDateTimeRenderOption') else ''
        }
        query_parameter = check_payload(query_parameter)
        payload = check_payload(params.get('data'))
        response = api_request('POST', url, connector_info, config, params=query_parameter, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def get_spreadsheet_rows(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/values/{2}'.format(SHEETS_API_VERSION, params.get('spreadsheetId'),
                                                       params.get('range'))
        query_parameter = {
            'majorDimension': MAPPING.get(params.get('majorDimension')) if params.get('majorDimension') else '',
            'valueRenderOption': MAPPING.get(params.get('valueRenderOption')) if params.get(
                'valueRenderOption') else '',
            'dateTimeRenderOption': MAPPING.get(params.get('dateTimeRenderOption')) if params.get(
                'dateTimeRenderOption') else ''
        }
        query_parameter = build_payload(query_parameter)
        response = api_request('GET', url, connector_info, config, params=query_parameter)
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def update_rows_in_spreadsheet(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/values:batchUpdate'.format(SHEETS_API_VERSION, params.get('spreadsheetId'))
        payload = {
            'data': params.get('data'),
            'valueInputOption': MAPPING.get(params.get('valueInputOption')) if params.get('valueInputOption') else '',
            'includeValuesInResponse': params.get('includeValuesInResponse'),
            'responseValueRenderOption': MAPPING.get(params.get('responseValueRenderOption')) if params.get(
                'responseValueRenderOption') else '',
            'responseDateTimeRenderOption': MAPPING.get(params.get('responseDateTimeRenderOption')) if params.get(
                'responseDateTimeRenderOption') else ''
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def update_rows_of_spreadsheet_by_filter(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/values:batchUpdateByDataFilter'.format(SHEETS_API_VERSION,
                                                                           params.get('spreadsheetId'))
        payload = {
            'data': params.get('data'),
            'valueInputOption': MAPPING.get(params.get('valueInputOption')) if params.get('valueInputOption') else '',
            'includeValuesInResponse': params.get('includeValuesInResponse'),
            'responseValueRenderOption': MAPPING.get(params.get('responseValueRenderOption')) if params.get(
                'responseValueRenderOption') else '',
            'responseDateTimeRenderOption': MAPPING.get(params.get('responseDateTimeRenderOption')) if params.get(
                'responseDateTimeRenderOption') else ''
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def clear_rows_from_spreadsheet(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/values:batchClear'.format(SHEETS_API_VERSION, params.get('spreadsheetId'))
        ranges = params.get('ranges')
        if ranges:
            ranges = ranges if type(ranges) == list else ranges.split(",")
        payload = {
            'ranges': ranges
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def clear_rows_of_spreadsheet_by_filter(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/values:batchClearByDataFilter'.format(SHEETS_API_VERSION,
                                                                          params.get('spreadsheetId'))
        payload = {
            'dataFilters': params.get('data')
        }
        payload = check_payload(payload)
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def move_sheet(config, params, connector_info):
    try:
        url = '{0}/spreadsheets/{1}/sheets/{2}:copyTo'.format(SHEETS_API_VERSION, params.get('spreadsheetId'),
                                                              params.get('sheetId'))
        payload = {'destinationSpreadsheetId': params.get('destinationSpreadsheetId')}
        response = api_request('POST', url, connector_info, config, data=json.dumps(payload))
        return response
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


def _check_health(config, connector_info):
    try:
        return check(config, connector_info)
    except Exception as err:
        logger.exception("{0}".format(str(err)))
        raise ConnectorError("{0}".format(str(err)))


operations = {
    'create_spreadsheet': create_spreadsheet,
    'get_spreadsheet_details': get_spreadsheet_details,
    'filter_spreadsheet': filter_spreadsheet,
    'add_row_to_spreadsheet': add_row_to_spreadsheet,
    'get_spreadsheet_rows': get_spreadsheet_rows,
    'update_rows_in_spreadsheet': update_rows_in_spreadsheet,
    'update_rows_of_spreadsheet_by_filter': update_rows_of_spreadsheet_by_filter,
    'clear_rows_from_spreadsheet': clear_rows_from_spreadsheet,
    'clear_rows_of_spreadsheet_by_filter': clear_rows_of_spreadsheet_by_filter,
    'move_sheet': move_sheet
}
