"""
This file is a standalone job description converter from the old description to a proposed one.
"""

import re


def camel_to_snake(name):
    """
    Changes CamelCase to snake_case, used by python.

    :param name: name to change
    :return: name in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def split(val, separator=","):
    """
    Splits comma separated values and parses them.

    :param val: values to split
    :param separator: comma or whatever
    :return: parsed values
    """
    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    return v_arr


def get_nulls(val):
    """
    Makes every "NULL" None.
    :param val: string or whatever
    :return: val or None if val is "NULL"
    """
    return val if val != "NULL" else None


key_fix = {
    'PandaID': 'job_id',  # it is job id, not PanDA
    'transformation': 'command',  # making it more convenient
    'jobPars': 'command_parameters',  # -.-
    'coreCount': 'cores_number',
    'prodUserID': 'user_dn',
    'prodSourceLabel': 'label',  # We don't have any other labels in there. And this is The Label, or just label
    'homepackage': 'home_package',  # lowercase, all of a sudden
    "nSent": 'sending_attempts',  # are they?
    'minRamCount': 'minimum_ram',  # reads better
    'maxDiskCount': 'maximum_disk_space',  # what does "count" mean? Partitions number? HDD's? Maybe number of disks
                                           # been used from the first installation of OS on that node?
    'attemptNr': 'attempt_number',  # bad practice to strip words API needs to be readable
    'prodDBlockToken': 'prod_dblock_token',  # camel_to_snake makes d_block, which is right, but heavier
}

arrays = []

skip_keys = [  # these are tested elsewhere
    'inFiles', "ddmEndPointIn", "destinationSE", "dispatchDBlockToken", "realDatasetsIn", "prodDBlocks", "fsize",
    "checksum", "outFiles", "ddmEndPointOut", "fileDestinationSE", "dispatchDBlockTokenForOut",
    "destinationDBlockToken", "realDatasets", "destinationDblock", "logGUID", "scopeIn", "scopeOut", "scopeLog"
]


def is_float(val):
    """
    Test floatliness of the value.
    :param val: string or whatever
    :return: True if the value may be converted to Float
    """
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_long(s):
    """
    Test value to be convertable to integer.
    :param s: string or whatever
    :return: True if the value may be converted to Long
    """
    if not isinstance(s, basestring):
        try:
            long(s)
            return True
        except ValueError:
            return False

    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()


def parse_value(value):
    """
    Tries to parse value as number or None. If some of this can be done, parsed value is returned. Otherwise returns
    value without parsing.

    :param value:
    :return: mixed
    """
    if not isinstance(value, basestring):
        return value
    if is_long(value):
        return long(value)
    if is_float(value):
        return float(value)
    return get_nulls(value)


def get_input_files(description):
    """
    Extracts input files from the description.
    :param description:
    :return: file list
    """
    files = {}
    if description['inFiles'] and description['inFiles'] != "NULL":
        in_files = split(description["inFiles"])
        ddm_endpoint = split(description["ddmEndPointIn"])
        destination_se = split(description["destinationSE"])
        dblock_token = split(description["dispatchDBlockToken"])
        datasets = split(description["realDatasetsIn"])
        dblocks = split(description["prodDBlocks"])
        size = split(description["fsize"])
        c_sum = split(description["checksum"])
        scope = parse_value(description["scopeIn"])

        for i, f in enumerate(in_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock_token": dblock_token[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "size": size[i],
                    "checksum": c_sum[i],
                    'scope': scope
                }
    return files


def fix_log(description, files):
    """
    Fixes log file description in output files (changes GUID and scope).
    :param description:
    :param files: output files
    :return: fixed output files
    """
    if description["logFile"] and description["logFile"] != "NULL":
        if description["logGUID"] and description["logGUID"] != "NULL" and description["logFile"] in\
                    files:
            files[description["logFile"]]["guid"] = description["logGUID"]
            files[description["logFile"]]["scope"] = description["scopeLog"]

    return files


def get_output_files(description):
    """
    Extracts output files from the description.
    :param description:
    :return: output files
    """
    files = {}
    if description['outFiles'] and description['outFiles'] != "NULL":
        out_files = split(description["outFiles"])
        ddm_endpoint = split(description["ddmEndPointOut"])
        destination_se = split(description["fileDestinationSE"])
        dblock_token = split(description["dispatchDBlockTokenForOut"])
        destination_dblock_token = split(description["destinationDBlockToken"])
        datasets = split(description["realDatasets"])
        dblocks = split(description["destinationDblock"])
        scope = parse_value(description["scopeOut"])

        for i, f in enumerate(out_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock_token": dblock_token[i],
                    "destination_dblock_token": destination_dblock_token[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "scope": scope
                }

    return fix_log(description, files)


def description_fixer(description):
    """
    Parses the description and changes it into more readable and usable way. For example, extracts all the files and
    makes a structure of them.
    :param description:
    :return: fixed description
    """
    fixed = {}
    if isinstance(description, basestring):
        import json
        description = json.loads(description)
    for key in description:
        value = description[key]

        fixed['input_files'] = get_input_files(description)
        fixed['output_files'] = get_output_files(description)

        if key not in skip_keys:
            if key in key_fix:
                key = key_fix[key]
            else:
                key = camel_to_snake(key)

            if key in arrays:
                fixed[key] = split(value)
            else:
                fixed[key] = parse_value(value)
    return fixed
